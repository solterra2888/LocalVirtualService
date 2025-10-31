#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 news 表的 analysis_result 字段同步 classification 数据到 news_themes_unified 表

用法:
    python scripts/sync_classification_from_analysis_result.py
    python scripts/sync_classification_from_analysis_result.py --limit 100
    python scripts/sync_classification_from_analysis_result.py --news-ids 1234,5678
    python scripts/sync_classification_from_analysis_result.py --force-update
"""

import sys
import os
import json
import re
import argparse
from typing import Dict, List, Any, Optional

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.services.theme_mapping_service import ThemeMappingService
from backend.utils.logger import get_logger

logger = get_logger("sync_classification")


def parse_json_content(content: str) -> Optional[Dict[str, Any]]:
    """
    解析 LLM 返回的 JSON 内容
    处理可能包含 markdown code block 的情况
    """
    if not content:
        return None
    
    # 去掉 markdown code block 标记
    content = content.strip()
    
    # 去掉开头的 ```json 或 ```
    if content.startswith('```json'):
        content = content[7:]  # 去掉 ```json
    elif content.startswith('```'):
        content = content[3:]   # 去掉 ```
    
    # 去掉结尾的 ```
    if content.endswith('```'):
        content = content[:-3]  # 去掉结尾 ```
    
    content = content.strip()
    
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON 解析失败: {str(e)}, content: {content[:200]}")
        return None


def extract_classification_data(analysis_result: Any) -> Optional[Dict[str, Any]]:
    """
    从 analysis_result JSONB 字段中提取 classification 数据
    
    Args:
        analysis_result: analysis_result 字段的值（可能是 dict 或 JSON 字符串）
    
    Returns:
        分类数据字典，格式：
        {
            "primary_theme": "healthcare",
            "event_type": "earnings_report",
            "impact_scope": "company_specific",
            "urgency": "timely",
            "related_sectors": ["healthcare", "finance"],
            "rationale": "...",
            "primary_theme_confidence": 0.95,
            "event_type_confidence": 0.88
        }
    """
    if not analysis_result:
        return None
    
    # 如果是字符串，先解析为字典
    if isinstance(analysis_result, str):
        try:
            analysis_result = json.loads(analysis_result)
        except json.JSONDecodeError:
            return None
    
    if not isinstance(analysis_result, dict):
        return None
    
    # 检查是否有 news_classification 字段
    classification_template = analysis_result.get('news_classification')
    if not classification_template:
        return None
    
    # 获取 content 字段
    content = classification_template.get('content')
    if not content:
        return None
    
    # 如果 content 是字符串，解析为字典
    if isinstance(content, str):
        content = parse_json_content(content)
    
    if not content or not isinstance(content, dict):
        return None
    
    # 提取分类数据
    classification_data = {
        "primary_theme": content.get('primary_theme'),
        "event_type": content.get('event_type'),
        "impact_scope": content.get('impact_scope'),
        "urgency": content.get('urgency'),
        "related_sectors": content.get('related_sectors', []),
        "rationale": content.get('rationale'),
        "analyzed_at": classification_template.get('analyzed_at')
    }
    
    # 提取置信度
    confidence_scores = {}
    if 'primary_theme_confidence' in content:
        confidence_scores['primary_theme_confidence'] = float(content['primary_theme_confidence'])
    if 'event_type_confidence' in content:
        confidence_scores['event_type_confidence'] = float(content['event_type_confidence'])
    
    # 如果没有任何分类数据，返回 None
    if not any([
        classification_data.get('primary_theme'),
        classification_data.get('event_type'),
        classification_data.get('impact_scope'),
        classification_data.get('urgency'),
        classification_data.get('related_sectors')
    ]):
        return None
    
    return {
        "classification_data": classification_data,
        "confidence_scores": confidence_scores
    }


def sync_news_classification(
    news_id: int,
    theme_service: ThemeMappingService,
    force_update: bool = False
) -> Dict[str, Any]:
    """
    同步单条新闻的 classification 数据
    
    Args:
        news_id: 新闻ID
        theme_service: ThemeMappingService 实例
        force_update: 是否强制更新（即使已存在）
    
    Returns:
        同步结果
    """
    try:
        # 获取数据库连接
        db_manager = theme_service.db_manager
        if not db_manager:
            return {'success': False, 'error': 'No database manager'}
        
        conn = db_manager.get_connection()
        if not conn:
            return {'success': False, 'error': 'Database connection failed'}
        
        cursor = conn.cursor()
        
        # 查询该新闻的 analysis_result
        cursor.execute("""
            SELECT id, analysis_result
            FROM news
            WHERE id = %s
              AND analysis_result IS NOT NULL
        """, (news_id,))
        
        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            return {'success': False, 'error': f'News {news_id} not found or has no analysis_result'}
        
        analysis_result = row[1]
        
        # 提取 classification 数据
        extracted = extract_classification_data(analysis_result)
        
        if not extracted:
            cursor.close()
            conn.close()
            return {'success': False, 'error': f'No classification data found in news {news_id}'}
        
        classification_data = extracted['classification_data']
        confidence_scores = extracted['confidence_scores']
        
        # 检查是否已存在（如果不强制更新）
        if not force_update:
            cursor.execute("""
                SELECT COUNT(*) 
                FROM news_themes_unified
                WHERE news_id = %s
                  AND source_type = 'llm_classification'
            """, (news_id,))
            
            existing_count = cursor.fetchone()[0]
            if existing_count > 0:
                cursor.close()
                conn.close()
                logger.debug(f"News {news_id} already has classification data, skipping")
                return {'success': True, 'skipped': True, 'message': 'Already exists'}
        
        cursor.close()
        conn.close()
        
        # 使用 ThemeMappingService 插入分类维度
        result = theme_service.upsert_classification_dimensions(
            news_id=news_id,
            classification_data=classification_data,
            confidence_scores=confidence_scores
        )
        
        if result.get('success'):
            logger.info(f"✅ News {news_id}: Synced {result.get('total_inserted', 0)} classification dimensions")
        else:
            logger.error(f"❌ News {news_id}: Failed to sync - {result.get('error', 'Unknown error')}")
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to sync news {news_id}: {str(e)}")
        return {'success': False, 'error': str(e)}


def main():
    parser = argparse.ArgumentParser(
        description='从 news 表的 analysis_result 同步 classification 数据到 news_themes_unified'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='限制处理的新闻数量（默认：全部）'
    )
    parser.add_argument(
        '--news-ids',
        type=str,
        default=None,
        help='指定要处理的新闻ID列表，逗号分隔（如：1234,5678）'
    )
    parser.add_argument(
        '--force-update',
        action='store_true',
        help='强制更新，即使已存在分类数据'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='仅显示将要处理的内容，不实际更新'
    )
    parser.add_argument(
        '--skip-existing',
        action='store_true',
        help='跳过已存在分类数据的新闻（默认：false，会更新已存在的）'
    )
    
    args = parser.parse_args()
    
    # 初始化服务
    logger.info("初始化服务...")
    db_config = get_database_config()
    db_manager = DatabaseManager(db_config)
    theme_service = ThemeMappingService(db_manager)
    
    # 获取要处理的新闻列表
    conn = db_manager.get_connection()
    if not conn:
        logger.error("数据库连接失败")
        return
    
    cursor = conn.cursor()
    
    try:
        if args.news_ids:
            # 处理指定的新闻ID
            news_ids = [int(id.strip()) for id in args.news_ids.split(',')]
            placeholders = ','.join(['%s'] * len(news_ids))
            query = f"""
                SELECT id, analysis_result
                FROM news
                WHERE id IN ({placeholders})
                  AND analysis_result IS NOT NULL
                ORDER BY id
            """
            cursor.execute(query, news_ids)
        else:
            # 查询所有有 classification 数据的新闻
            query = """
                SELECT id, analysis_result
                FROM news
                WHERE analysis_result IS NOT NULL
                  AND analysis_result->'news_classification' IS NOT NULL
                  AND analysis_result->'news_classification'->'content' IS NOT NULL
                ORDER BY id
            """
            
            if args.limit:
                query += f" LIMIT {args.limit}"
            
            cursor.execute(query)
        
        rows = cursor.fetchall()
        
        logger.info(f"找到 {len(rows)} 条包含 classification 数据的新闻")
        
        if args.dry_run:
            logger.info("=" * 80)
            logger.info("DRY RUN 模式：仅显示将要处理的内容")
            logger.info("=" * 80)
            
            for row in rows[:10]:  # 只显示前10条
                news_id = row[0]
                analysis_result = row[1]
                extracted = extract_classification_data(analysis_result)
                
                if extracted:
                    logger.info(f"News {news_id}: {extracted['classification_data'].get('primary_theme', 'N/A')}")
            
            logger.info(f"\n总共将处理 {len(rows)} 条新闻")
            return
        
        # 处理每条新闻
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        logger.info("=" * 80)
        logger.info("开始同步 classification 数据...")
        logger.info("=" * 80)
        
        for i, row in enumerate(rows, 1):
            news_id = row[0]
            
            logger.info(f"[{i}/{len(rows)}] 处理新闻 {news_id}...")
            
            result = sync_news_classification(
                news_id=news_id,
                theme_service=theme_service,
                force_update=args.force_update
            )
            
            if result.get('success'):
                if result.get('skipped'):
                    skipped_count += 1
                else:
                    success_count += 1
            else:
                failed_count += 1
                logger.error(f"  ❌ 失败: {result.get('error', 'Unknown error')}")
        
        # 输出统计
        logger.info("")
        logger.info("=" * 80)
        logger.info("同步完成！")
        logger.info("=" * 80)
        logger.info(f"总计: {len(rows)} 条")
        logger.info(f"成功: {success_count} 条")
        logger.info(f"跳过: {skipped_count} 条")
        logger.info(f"失败: {failed_count} 条")
        logger.info("=" * 80)
        
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()

