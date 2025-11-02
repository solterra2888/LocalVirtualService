# -*- coding: utf-8 -*-
"""
主题映射服务 - 将 LLM 的 primary_theme 映射到 theme_master_data
"""

from typing import Optional, Dict, Any
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("theme_mapping_service")


class ThemeMappingService:
    """主题映射服务 - 简化的主题匹配逻辑"""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        初始化主题映射服务
        
        Args:
            db_manager: 数据库管理器实例（可选）
        """
        self.db_manager = db_manager
    
    def get_theme_master_id(self, category: str) -> Optional[int]:
        """
        根据 category 获取 theme_master_id
        
        Args:
            category: 主题分类（如：technology, finance）
            
        Returns:
            theme_master_id 或 None
        """
        if not self.db_manager:
            return None
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("Database connection failed")
                return None
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id 
                FROM theme_master_data 
                WHERE category = %s 
                  AND is_active = true 
                  AND is_system = true
                LIMIT 1
            """, (category,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                return result[0]
            else:
                logger.warning(f"No theme_master_id found for category: {category}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to get theme_master_id for category {category}: {str(e)}")
            return None
    
    def upsert_primary_theme(
        self,
        news_id: int,
        primary_theme: str,
        confidence_score: float = 0.8,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[int]:
        """
        插入或更新新闻的 primary theme
        
        Args:
            news_id: 新闻ID
            primary_theme: 主题分类（如：technology）
            confidence_score: 置信度分数
            metadata: 额外的元数据
            
        Returns:
            插入的记录ID 或 None
        """
        if not self.db_manager:
            return None
        
        try:
            # 1. 获取 theme_master_id
            theme_master_id = self.get_theme_master_id(primary_theme)
            if not theme_master_id:
                logger.error(f"Invalid primary_theme: {primary_theme}")
                return None
            
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("Database connection failed")
                return None
            
            cursor = conn.cursor()
            
            # 2. 先取消该新闻的其他 primary theme
            cursor.execute("""
                UPDATE news_themes_unified 
                SET is_primary = false
                WHERE news_id = %s 
                  AND is_primary = true
            """, (news_id,))
            
            # 3. 插入或更新当前的 primary theme
            import json
            metadata_json = json.dumps(metadata or {})
            
            # 确保 market 不为 NULL
            market_value = 'GLOBAL'
            
            cursor.execute("""
                INSERT INTO news_themes_unified (
                    news_id,
                    theme_master_id,
                    theme_name,
                    source_type,
                    confidence_score,
                    is_primary,
                    is_active,
                    metadata,
                    market
                ) VALUES (
                    %s, %s, %s, 'llm_classification', %s, true, true, %s::jsonb, %s
                )
                ON CONFLICT (news_id, source_type, theme_name, market)
                DO UPDATE SET
                    theme_master_id = EXCLUDED.theme_master_id,
                    confidence_score = EXCLUDED.confidence_score,
                    is_primary = true,
                    is_active = true,
                    metadata = EXCLUDED.metadata,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                news_id,
                theme_master_id,
                primary_theme,
                confidence_score,
                metadata_json,
                market_value
            ))
            
            result = cursor.fetchone()
            record_id = result[0] if result else None
            
            conn.commit()
            
            # 🔥 新增：同步 theme_master_id 到同一个 news_id 的其他记录
            if record_id and theme_master_id:
                try:
                    cursor.execute("""
                        UPDATE news_themes_unified 
                        SET theme_master_id = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE news_id = %s 
                          AND theme_master_id IS NULL
                          AND is_active = true
                    """, (theme_master_id, news_id))
                    
                    updated_count = cursor.rowcount
                    conn.commit()
                    
                    if updated_count > 0:
                        logger.info(f"  🔗 同步 theme_master_id 到同一新闻的其他 {updated_count} 条记录")
                except Exception as e:
                    logger.warning(f"  ⚠️  同步 theme_master_id 失败: {str(e)}")
                    # 不影响主流程，继续执行
            
            cursor.close()
            conn.close()
            
            if record_id:
                logger.info(f"✅ Primary theme set for news {news_id}: {primary_theme} (confidence: {confidence_score})")
            
            return record_id
            
        except Exception as e:
            logger.error(f"Failed to upsert primary theme: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return None
    
    def get_primary_theme(self, news_id: int) -> Optional[Dict[str, Any]]:
        """
        获取新闻的 primary theme
        
        Args:
            news_id: 新闻ID
            
        Returns:
            主题信息字典 或 None
        """
        if not self.db_manager:
            return None
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return None
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    ntu.theme_master_id,
                    tmd.category as primary_theme,
                    tmd.display_name,
                    tmd.description,
                    ntu.confidence_score,
                    ntu.metadata,
                    ntu.created_at
                FROM news_themes_unified ntu
                INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
                WHERE ntu.news_id = %s 
                  AND ntu.is_primary = true
                  AND ntu.is_active = true
                LIMIT 1
            """, (news_id,))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                return {
                    'theme_master_id': result[0],
                    'primary_theme': result[1],
                    'display_name': result[2],
                    'description': result[3],
                    'confidence_score': float(result[4]) if result[4] else None,
                    'metadata': result[5],
                    'created_at': result[6].isoformat() if result[6] else None
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get primary theme for news {news_id}: {str(e)}")
            return None
    
    def get_all_standard_categories(self) -> list:
        """
        获取所有标准主题分类
        
        Returns:
            分类列表
        """
        if not self.db_manager:
            return []
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return []
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    id,
                    category,
                    display_name,
                    description,
                    priority
                FROM theme_master_data
                WHERE is_system = true
                  AND is_active = true
                ORDER BY priority DESC
            """)
            
            categories = []
            for row in cursor.fetchall():
                categories.append({
                    'id': row[0],
                    'category': row[1],
                    'display_name': row[2],
                    'description': row[3],
                    'priority': row[4]
                })
            
            cursor.close()
            conn.close()
            
            return categories
            
        except Exception as e:
            logger.error(f"Failed to get standard categories: {str(e)}")
            return []
    
    def upsert_classification_dimensions(
        self,
        news_id: int,
        classification_data: Dict[str, Any],
        confidence_scores: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """
        插入或更新 news_classification 的所有维度
        
        Args:
            news_id: 新闻ID
            classification_data: 分类数据，格式：
                {
                    "primary_theme": "healthcare",
                    "event_type": "product_launch",
                    "impact_scope": "company_specific",
                    "urgency": "timely",
                    "related_sectors": ["healthcare", "technology", "finance"],
                    "related_countries": ["usa", "china", "japan"]
                }
            confidence_scores: 各维度的置信度，格式：
                {
                    "primary_theme_confidence": 0.95,
                    "event_type_confidence": 0.88
                }
        
        Returns:
            插入结果统计
        """
        if not self.db_manager:
            return {'success': False, 'error': 'No database manager'}
        
        confidence_scores = confidence_scores or {}
        inserted_records = []
        errors = []
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return {'success': False, 'error': 'Database connection failed'}
            
            cursor = conn.cursor()
            
            import json
            
            # 1. 先删除该新闻的所有旧 llm_classification 记录
            # 注意：由于我们要重新插入，先清理旧数据避免冲突
            # 删除后重新插入可以确保数据一致性和避免唯一约束冲突
            cursor.execute("""
                DELETE FROM news_themes_unified
                WHERE news_id = %s
                  AND source_type = 'llm_classification'
            """, (news_id,))
            
            # 2. 准备要插入的各个维度
            dimensions_to_insert = []
            
            # 2.1 Primary Theme
            if classification_data.get('primary_theme'):
                theme_master_id = self.get_theme_master_id(classification_data['primary_theme'])
                dimensions_to_insert.append({
                    'title': 'primary_theme',
                    'value': classification_data['primary_theme'],
                    'theme_master_id': theme_master_id,
                    'confidence': confidence_scores.get('primary_theme_confidence', 0.8),
                    'is_primary': True
                })
            
            # 2.2 Event Type
            if classification_data.get('event_type'):
                dimensions_to_insert.append({
                    'title': 'event_type',
                    'value': classification_data['event_type'],
                    'theme_master_id': None,
                    'confidence': confidence_scores.get('event_type_confidence', 0.8),
                    'is_primary': False
                })
            
            # 2.3 Impact Scope
            if classification_data.get('impact_scope'):
                dimensions_to_insert.append({
                    'title': 'impact_scope',
                    'value': classification_data['impact_scope'],
                    'theme_master_id': None,
                    'confidence': 0.8,
                    'is_primary': False
                })
            
            # 2.4 Urgency
            if classification_data.get('urgency'):
                dimensions_to_insert.append({
                    'title': 'urgency',
                    'value': classification_data['urgency'],
                    'theme_master_id': None,
                    'confidence': 0.8,
                    'is_primary': False
                })
            
            # 2.5 Related Sectors (数组，每个元素一条记录)
            related_sectors = classification_data.get('related_sectors', [])
            if isinstance(related_sectors, list):
                for sector in related_sectors:
                    if sector:  # 跳过空值
                        dimensions_to_insert.append({
                            'title': 'related_sectors',
                            'value': sector,
                            'theme_master_id': None,
                            'confidence': 0.8,
                            'is_primary': False
                        })
            
            # 2.6 Related Countries (数组，每个元素一条记录)
            related_countries = classification_data.get('related_countries', [])
            if isinstance(related_countries, list):
                for country in related_countries:
                    if country:  # 跳过空值
                        dimensions_to_insert.append({
                            'title': 'related_countries',
                            'value': country,
                            'theme_master_id': None,
                            'confidence': 0.8,
                            'is_primary': False
                        })
            
            # 3. 批量插入
            metadata = {
                'analyzed_at': classification_data.get('analyzed_at'),
                'rationale': classification_data.get('rationale'),
                'full_classification': classification_data
            }
            metadata_json = json.dumps(metadata)
            
            for dim in dimensions_to_insert:
                # 🔥 使用 SAVEPOINT 机制：每个插入操作独立，失败不影响后续操作
                savepoint_name = f"dim_{dim['title']}_{len(inserted_records)}"
                try:
                    cursor.execute(f"SAVEPOINT {savepoint_name}")
                    
                    # 对于有 llm_classification_title 的维度，使用 unique_news_classification_dimension 约束
                    # 对于 related_sectors 和 related_countries 这种多个值的维度，使用 theme_name 来区分
                    if dim['title'] in ['related_sectors', 'related_countries']:
                        # related_sectors 使用通用约束 (news_id, source_type, theme_name, market)
                        cursor.execute("""
                            INSERT INTO news_themes_unified (
                                news_id,
                                theme_master_id,
                                theme_name,
                                llm_classification_title,
                                source_type,
                                confidence_score,
                                is_primary,
                                is_active,
                                metadata,
                                market
                            ) VALUES (
                                %s, %s, %s, %s, 'llm_classification', %s, %s, true, %s::jsonb, 'GLOBAL'
                            )
                            ON CONFLICT (news_id, source_type, theme_name, market)
                            DO UPDATE SET
                                confidence_score = EXCLUDED.confidence_score,
                                metadata = EXCLUDED.metadata,
                                updated_at = CURRENT_TIMESTAMP
                            RETURNING id
                        """, (
                            news_id,
                            dim['theme_master_id'],
                            dim['value'],
                            dim['title'],
                            dim['confidence'],
                            dim['is_primary'],
                            metadata_json
                        ))
                    else:
                        # 其他维度使用 unique_news_classification_dimension 约束
                        cursor.execute("""
                            INSERT INTO news_themes_unified (
                                news_id,
                                theme_master_id,
                                theme_name,
                                llm_classification_title,
                                source_type,
                                confidence_score,
                                is_primary,
                                is_active,
                                metadata,
                                market
                            ) VALUES (
                                %s, %s, %s, %s, 'llm_classification', %s, %s, true, %s::jsonb, 'GLOBAL'
                            )
                            ON CONFLICT (news_id, source_type, llm_classification_title, theme_name, market)
                            DO UPDATE SET
                                theme_master_id = EXCLUDED.theme_master_id,
                                confidence_score = EXCLUDED.confidence_score,
                                is_primary = EXCLUDED.is_primary,
                                metadata = EXCLUDED.metadata,
                                updated_at = CURRENT_TIMESTAMP
                            RETURNING id
                        """, (
                            news_id,
                            dim['theme_master_id'],
                            dim['value'],
                            dim['title'],
                            dim['confidence'],
                            dim['is_primary'],
                            metadata_json
                        ))
                    
                    result = cursor.fetchone()
                    if result:
                        inserted_records.append({
                            'id': result[0],
                            'title': dim['title'],
                            'value': dim['value']
                        })
                        logger.debug(f"  ✅ Inserted/Updated {dim['title']}: {dim['value']}")
                    
                    # 释放 savepoint（成功）
                    cursor.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    
                except Exception as e:
                    # 回滚到 savepoint，不影响其他操作
                    try:
                        cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    except:
                        pass  # savepoint 可能已经失效
                    
                    # 清理错误信息中的换行符，使日志更清晰
                    error_msg = str(e).replace('\n', ' ').strip()
                    error_detail = f"Failed to insert {dim['title']}={dim['value']}: {error_msg}"
                    errors.append(error_detail)
                    logger.error(f"  ❌ {error_detail}")
                    # 继续处理下一个维度，不中断事务
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Inserted {len(inserted_records)} classification dimensions for news {news_id}")
            
            return {
                'success': True,
                'news_id': news_id,
                'total_inserted': len(inserted_records),
                'records': inserted_records,
                'errors': errors if errors else None
            }
            
        except Exception as e:
            logger.error(f"Failed to upsert classification dimensions for news {news_id}: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_classification_dimensions(self, news_id: int) -> Optional[Dict[str, Any]]:
        """
        获取新闻的所有分类维度
        
        Args:
            news_id: 新闻ID
            
        Returns:
            分类维度字典
        """
        if not self.db_manager:
            return None
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return None
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    llm_classification_title,
                    theme_name,
                    confidence_score,
                    is_primary
                FROM news_themes_unified
                WHERE news_id = %s
                  AND source_type = 'llm_classification'
                  AND is_active = true
                ORDER BY is_primary DESC, created_at ASC
            """, (news_id,))
            
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not rows:
                return None
            
            # 组织数据
            result = {
                'primary_theme': None,
                'event_type': None,
                'impact_scope': None,
                'urgency': None,
                'related_sectors': [],
                'related_countries': []
            }
            
            for row in rows:
                title = row[0]
                value = row[1]
                confidence = float(row[2]) if row[2] else None
                
                if title == 'primary_theme':
                    result['primary_theme'] = value
                    result['primary_theme_confidence'] = confidence
                elif title == 'event_type':
                    result['event_type'] = value
                    result['event_type_confidence'] = confidence
                elif title == 'impact_scope':
                    result['impact_scope'] = value
                elif title == 'urgency':
                    result['urgency'] = value
                elif title == 'related_sectors':
                    result['related_sectors'].append(value)
                elif title == 'related_countries':
                    result['related_countries'].append(value)
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get classification dimensions for news {news_id}: {str(e)}")
            return None
    
    def batch_upsert_primary_themes(
        self,
        theme_mappings: list
    ) -> Dict[str, Any]:
        """
        批量插入或更新多条新闻的 primary theme
        
        Args:
            theme_mappings: 主题映射列表，格式：
                [
                    {
                        'news_id': 123,
                        'primary_theme': 'technology',
                        'confidence_score': 0.85,
                        'metadata': {...}
                    },
                    ...
                ]
                
        Returns:
            结果统计
        """
        success_count = 0
        failed_count = 0
        failed_ids = []
        
        for mapping in theme_mappings:
            try:
                result = self.upsert_primary_theme(
                    news_id=mapping['news_id'],
                    primary_theme=mapping['primary_theme'],
                    confidence_score=mapping.get('confidence_score', 0.8),
                    metadata=mapping.get('metadata')
                )
                
                if result:
                    success_count += 1
                else:
                    failed_count += 1
                    failed_ids.append(mapping['news_id'])
                    
            except Exception as e:
                logger.error(f"Failed to process news {mapping['news_id']}: {str(e)}")
                failed_count += 1
                failed_ids.append(mapping['news_id'])
        
        return {
            'total': len(theme_mappings),
            'success': success_count,
            'failed': failed_count,
            'failed_ids': failed_ids
        }



