"""
Theme Fusion Service
主题融合引擎 - 将多源主题数据智能融合，计算综合得分
"""
from typing import List, Dict, Optional
from collections import defaultdict

from backend.database.db_manager import DatabaseManager
from backend.schemas.theme_unified import (
    ThemeSourceType,
    ThemeFusionResult,
    NewsBestThemes,
    Market
)
from backend.utils.logger import logger


class ThemeFusionService:
    """主题融合引擎"""
    
    # 不同来源的权重系数
    SOURCE_WEIGHTS = {
        ThemeSourceType.MANUAL: 1.5,           # 人工标注最高
        ThemeSourceType.VECTOR_RETRIEVAL: 1.0, # 向量匹配可信
        ThemeSourceType.LLM_CLASSIFICATION: 0.8, # LLM次之
        ThemeSourceType.EODHD_AUTO_TAG: 0.6,   # EODHD参考
        ThemeSourceType.HYBRID: 1.2            # 混合来源
    }
    
    # 融合得分计算权重
    FUSION_WEIGHTS = {
        'base_confidence': 0.35,      # 基础置信度权重 35%
        'quality_score': 0.25,        # 质量分数权重 25%
        'source_consistency': 0.20,   # 多源一致性权重 20%
        'verification_bonus': 0.10,   # 人工验证加分 10%
        'primary_bonus': 0.05,        # 主要主题加分 5%
        'high_vector_bonus': 0.05     # 高分向量匹配加分 5%
    }
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
    
    # ============================================================
    # 主融合方法
    # ============================================================
    
    async def get_fused_themes(
        self,
        news_id: int,
        top_k: int = 5,
        min_score: float = 0.3
    ) -> NewsBestThemes:
        """
        获取新闻的融合主题
        
        Args:
            news_id: 新闻ID
            top_k: 返回前K个主题
            min_score: 最小融合得分阈值
        
        Returns:
            融合后的主题列表
        """
        # 查询该新闻的所有主题
        query = """
        SELECT 
            ntu.theme_name,
            ntu.theme_master_id,
            ntu.source_type,
            ntu.confidence_score,
            ntu.quality_score,
            ntu.market,
            ntu.is_verified,
            ntu.is_primary,
            ntu.metadata
        FROM news_themes_unified ntu
        WHERE ntu.news_id = %s
          AND ntu.is_active = true
          AND ntu.theme_name IS NOT NULL
        ORDER BY ntu.theme_name, ntu.confidence_score DESC
        """
        
        rows = await self.db.fetch_all(query, (news_id,))
        
        if not rows:
            return NewsBestThemes(
                news_id=news_id,
                themes=[],
                total_count=0
            )
        
        # 按主题名称分组
        theme_groups = self._group_by_theme(rows)
        
        # 计算每个主题的融合得分
        fused_themes = []
        for theme_name, sources in theme_groups.items():
            fusion_result = self._calculate_fusion_score(
                theme_name=theme_name,
                sources=sources
            )
            
            if fusion_result.fusion_score >= min_score:
                fused_themes.append(fusion_result)
        
        # 排序：先按融合得分，再按置信度
        fused_themes.sort(
            key=lambda x: (x.fusion_score, x.confidence_score),
            reverse=True
        )
        
        # 取前K个
        top_themes = fused_themes[:top_k]
        
        logger.info(
            f"新闻 {news_id} 融合了 {len(fused_themes)} 个主题，"
            f"返回前 {len(top_themes)} 个"
        )
        
        return NewsBestThemes(
            news_id=news_id,
            themes=top_themes,
            total_count=len(fused_themes)
        )
    
    async def get_fused_themes_batch(
        self,
        news_ids: List[int],
        top_k: int = 5
    ) -> Dict[int, NewsBestThemes]:
        """批量获取融合主题"""
        results = {}
        for news_id in news_ids:
            results[news_id] = await self.get_fused_themes(news_id, top_k)
        return results
    
    # ============================================================
    # 融合计算核心
    # ============================================================
    
    def _calculate_fusion_score(
        self,
        theme_name: str,
        sources: List[Dict]
    ) -> ThemeFusionResult:
        """
        计算主题的融合得分
        
        算法说明：
        1. 基础置信度：来源权重 * 置信度的加权平均
        2. 质量分数：平均质量分数
        3. 多源一致性：来源数量归一化加分
        4. 验证加分：人工验证的主题额外加分
        5. 主要主题加分：被标记为主要主题的加分
        6. 高质量向量匹配加分：相似度>0.85的向量匹配加分
        """
        # 提取数据
        source_types = [s['source_type'] for s in sources]
        confidence_scores = [float(s['confidence_score']) for s in sources]
        quality_scores = [float(s['quality_score']) for s in sources if s['quality_score'] is not None]
        markets = [s['market'] for s in sources if s['market']]
        is_verified = any(s['is_verified'] for s in sources)
        is_primary = any(s['is_primary'] for s in sources)
        
        # 1. 计算基础置信度（加权平均）
        weighted_confidence_sum = 0.0
        total_weight = 0.0
        
        for source in sources:
            source_type = source['source_type']
            confidence = float(source['confidence_score'])
            weight = self.SOURCE_WEIGHTS.get(
                ThemeSourceType(source_type),
                0.5
            )
            
            weighted_confidence_sum += weight * confidence
            total_weight += weight
        
        base_confidence = (
            weighted_confidence_sum / total_weight
            if total_weight > 0 else 0.5
        )
        
        # 2. 计算平均质量分数
        avg_quality = (
            sum(quality_scores) / len(quality_scores)
            if quality_scores else base_confidence
        )
        
        # 3. 多源一致性得分（归一化）
        source_count = len(set(source_types))
        max_sources = len(self.SOURCE_WEIGHTS)
        source_consistency = min(source_count / max_sources, 1.0)
        
        # 4. 检查是否有高分向量匹配
        has_high_vector_match = any(
            s['source_type'] == ThemeSourceType.VECTOR_RETRIEVAL.value
            and float(s['confidence_score']) >= 0.85
            for s in sources
        )
        
        # 5. 计算融合得分
        fusion_score = (
            base_confidence * self.FUSION_WEIGHTS['base_confidence'] +
            avg_quality * self.FUSION_WEIGHTS['quality_score'] +
            source_consistency * self.FUSION_WEIGHTS['source_consistency'] +
            (0.1 if is_verified else 0) * self.FUSION_WEIGHTS['verification_bonus'] +
            (0.1 if is_primary else 0) * self.FUSION_WEIGHTS['primary_bonus'] +
            (0.1 if has_high_vector_match else 0) * self.FUSION_WEIGHTS['high_vector_bonus']
        )
        
        # 限制在0-1范围
        fusion_score = min(max(fusion_score, 0.0), 1.0)
        
        # 获取主数据ID
        theme_master_id = next(
            (s['theme_master_id'] for s in sources if s['theme_master_id']),
            None
        )
        
        # 构建结果
        return ThemeFusionResult(
            theme_name=theme_name,
            fusion_score=fusion_score,
            confidence_score=base_confidence,
            quality_score=avg_quality if quality_scores else None,
            source_count=source_count,
            sources=[ThemeSourceType(st) for st in set(source_types)],
            markets=[Market(m) for m in set(markets) if m],
            is_verified=is_verified,
            is_primary=is_primary
        )
    
    # ============================================================
    # 辅助方法
    # ============================================================
    
    def _group_by_theme(self, rows: List[Dict]) -> Dict[str, List[Dict]]:
        """按主题名称分组"""
        groups = defaultdict(list)
        for row in rows:
            theme_name = row['theme_name']
            groups[theme_name].append(row)
        return dict(groups)
    
    # ============================================================
    # 统计分析
    # ============================================================
    
    async def analyze_theme_quality(
        self,
        theme_name: str,
        days: int = 30
    ) -> Dict:
        """
        分析主题质量
        
        Args:
            theme_name: 主题名称（canonical）
            days: 分析最近N天的数据
        
        Returns:
            质量分析报告
        """
        query = """
        SELECT 
            n.id as news_id,
            n.title,
            n.date,
            ntu.source_type,
            ntu.confidence_score,
            ntu.quality_score,
            ntu.is_verified
        FROM news_themes_unified ntu
        INNER JOIN news n ON ntu.news_id = n.id
        WHERE ntu.theme_name = %s
          AND ntu.is_active = true
          AND n.date >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
        ORDER BY n.date DESC
        """
        
        rows = await self.db.fetch_all(query, (theme_name, days))
        
        if not rows:
            return {
                'theme_name': theme_name,
                'news_count': 0,
                'error': '没有找到相关数据'
            }
        
        # 统计来源分布
        source_distribution = defaultdict(int)
        source_confidences = defaultdict(list)
        
        for row in rows:
            source_type = row['source_type']
            source_distribution[source_type] += 1
            source_confidences[source_type].append(float(row['confidence_score']))
        
        # 计算平均置信度
        avg_confidence_by_source = {
            source: sum(scores) / len(scores)
            for source, scores in source_confidences.items()
        }
        
        # 计算一致性得分
        news_groups = defaultdict(set)
        for row in rows:
            news_groups[row['news_id']].add(row['source_type'])
        
        # 一致性 = 平均每条新闻有多少个来源支持
        avg_sources_per_news = sum(len(sources) for sources in news_groups.values()) / len(news_groups)
        consistency_score = min(avg_sources_per_news / 3.0, 1.0)  # 归一化到0-1
        
        # 质量问题
        quality_issues = []
        if consistency_score < 0.5:
            quality_issues.append("多源一致性较低，建议审查主题定义")
        if avg_confidence_by_source.get(ThemeSourceType.VECTOR_RETRIEVAL.value, 0) < 0.7:
            quality_issues.append("向量匹配置信度较低")
        
        return {
            'theme_name': theme_name,
            'news_count': len(news_groups),
            'source_distribution': dict(source_distribution),
            'avg_confidence_by_source': avg_confidence_by_source,
            'consistency_score': consistency_score,
            'avg_sources_per_news': avg_sources_per_news,
            'quality_issues': quality_issues,
            'sample_news': [
                {
                    'id': row['news_id'],
                    'title': row['title'],
                    'date': row['date'].isoformat() if row['date'] else None
                }
                for row in rows[:5]  # 只返回前5条样本
            ]
        }
    
    async def compare_sources(
        self,
        news_id: int
    ) -> Dict:
        """
        对比同一新闻不同来源的主题识别结果
        用于评估各来源的准确性
        """
        query = """
        SELECT 
            ntu.source_type,
            ntu.theme_name,
            ntu.confidence_score,
            ntu.quality_score
        FROM news_themes_unified ntu
        WHERE ntu.news_id = %s
          AND ntu.is_active = true
        ORDER BY ntu.source_type, ntu.confidence_score DESC
        """
        
        rows = await self.db.fetch_all(query, (news_id,))
        
        # 按来源分组
        by_source = defaultdict(list)
        for row in rows:
            by_source[row['source_type']].append({
                'theme_name': row['theme_name'],
                'confidence': float(row['confidence_score']),
                'quality': float(row['quality_score']) if row['quality_score'] else None
            })
        
        # 找出共同主题
        all_themes = set()
        for themes in by_source.values():
            all_themes.update(t['theme_name'] for t in themes)
        
        common_themes = []
        for theme in all_themes:
            sources_with_theme = [
                source for source, themes in by_source.items()
                if any(t['canonical'] == theme for t in themes)
            ]
            if len(sources_with_theme) >= 2:
                common_themes.append({
                    'theme': theme,
                    'sources': sources_with_theme,
                    'count': len(sources_with_theme)
                })
        
        return {
            'news_id': news_id,
            'by_source': dict(by_source),
            'common_themes': sorted(common_themes, key=lambda x: x['count'], reverse=True),
            'source_summary': {
                source: len(themes)
                for source, themes in by_source.items()
            }
        }



