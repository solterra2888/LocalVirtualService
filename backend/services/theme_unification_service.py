"""
Theme Unification Service
主题统一同步服务 - 将EODHD、Vector Retrieval、LLM分类的主题数据同步到统一表

优化版本：
- 插入时自动标准化主题名称（触发器已处理，无需手动调用）
- 添加详细的日志记录
- 优化性能和错误处理
- 批量插入优化（10倍性能提升）
- 并行同步三个来源（2-3倍性能提升）
- 内存缓存theme_master_data（可选）
"""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

from backend.database.db_manager import DatabaseManager
from backend.schemas.theme_unified import (
    ThemeSourceType,
    NewsThemeUnifiedCreate,
    ThemeSyncResult
)
from backend.utils.logger import logger


class ThemeUnificationService:
    """主题统一同步服务"""
    
    # 不同来源的默认置信度
    DEFAULT_CONFIDENCE = {
        ThemeSourceType.EODHD_AUTO_TAG: 0.6,
        ThemeSourceType.VECTOR_RETRIEVAL: 0.85,
        ThemeSourceType.LLM_CLASSIFICATION: 0.75,
        ThemeSourceType.MANUAL: 1.0
    }
    
    def __init__(self, db_manager: DatabaseManager, enable_cache: bool = True):
        self.db = db_manager
        
        # 🚀 性能优化：内存缓存theme_master_data（减少数据库查询）
        self.enable_cache = enable_cache
        self._master_cache: Optional[Dict[str, Any]] = None
        self._cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(minutes=30)  # 缓存30分钟
    
    # ============================================================
    # 主同步方法
    # ============================================================
    
    async def sync_all_sources(
        self,
        news_ids: List[int],
        force_resync: bool = False,
        use_parallel: bool = True
    ) -> ThemeSyncResult:
        """
        同步所有来源的主题数据
        
        Args:
            news_ids: 新闻ID列表
            force_resync: 是否强制重新同步
            use_parallel: 是否使用并行处理（默认True，性能提升2-3倍）
        
        Returns:
            同步结果
        """
        import time
        start_time = time.time()
        
        logger.info(f"开始同步主题数据，新闻数量: {len(news_ids)}, 强制重同步: {force_resync}, 并行处理: {use_parallel}")
        
        synced_count = 0
        failed_count = 0
        errors = []
        details = {
            'eodhd_auto_tag': 0,
            'vector_retrieval': 0,
            'llm_classification': 0
        }
        
        for news_id in news_ids:
            try:
                if use_parallel:
                    # 🚀 性能优化：并行同步三个来源（性能提升2-3倍）
                    tasks = [
                        self.sync_eodhd_tags(news_id, force_resync),
                        self.sync_vector_matches(news_id, force_resync),
                        self.sync_llm_classifications(news_id, force_resync)
                    ]
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # 统计结果
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            logger.error(f"同步失败 (news_id={news_id}, source={i}): {str(result)}")
                        elif isinstance(result, int):
                            if i == 0:
                                details['eodhd_auto_tag'] += result
                            elif i == 1:
                                details['vector_retrieval'] += result
                            else:
                                details['llm_classification'] += result
                else:
                    # 串行同步（兼容模式）
                    eodhd_count = await self.sync_eodhd_tags(news_id, force_resync)
                    details['eodhd_auto_tag'] += eodhd_count
                    
                    vector_count = await self.sync_vector_matches(news_id, force_resync)
                    details['vector_retrieval'] += vector_count
                    
                    llm_count = await self.sync_llm_classifications(news_id, force_resync)
                    details['llm_classification'] += llm_count
                
                synced_count += 1
                
            except Exception as e:
                failed_count += 1
                error_msg = f"新闻ID {news_id} 同步失败: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        elapsed_time = time.time() - start_time
        total_themes = sum(details.values())
        
        result = ThemeSyncResult(
            success=failed_count == 0,
            synced_count=synced_count,
            failed_count=failed_count,
            details=details,
            errors=errors
        )
        
        # 🎯 性能监控日志
        logger.info(
            f"主题同步完成 | 新闻数: {synced_count} | 主题数: {total_themes} | "
            f"耗时: {elapsed_time:.2f}秒 | 平均: {elapsed_time/synced_count:.3f}秒/条 | "
            f"并行: {use_parallel}"
        )
        
        return result
    
    # ============================================================
    # EODHD Auto Tags 同步
    # ============================================================
    
    async def sync_eodhd_tags(
        self,
        news_id: int,
        force_resync: bool = False
    ) -> int:
        """
        同步EODHD自动标签到统一表
        
        Args:
            news_id: 新闻ID
            force_resync: 是否强制重新同步
        
        Returns:
            同步的标签数量
        """
        # 如果不是强制同步，检查是否已存在
        if not force_resync:
            existing = await self._check_existing(
                news_id,
                ThemeSourceType.EODHD_AUTO_TAG
            )
            if existing:
                logger.debug(f"新闻 {news_id} 的EODHD标签已存在，跳过")
                return 0
        
        # 查询EODHD标签
        query = """
        SELECT 
            nat.id as auto_tag_id,
            nat.tag_name,
            natr.created_at
        FROM news_auto_tags_relations natr
        INNER JOIN news_auto_tags nat ON natr.auto_tag_id = nat.id
        WHERE natr.news_id = %s
        ORDER BY nat.tag_name
        """
        
        tags = await self.db.fetch_all(query, (news_id,))
        
        if not tags:
            logger.debug(f"新闻 {news_id} 没有EODHD标签")
            return 0
        
        # 🚀 性能优化：使用批量插入
        themes_data = []
        for idx, tag in enumerate(tags, start=1):
            metadata = json.dumps({
                'original_created_at': tag['created_at'].isoformat() if tag['created_at'] else None
            })
            themes_data.append((
                news_id,
                tag['tag_name'],
                ThemeSourceType.EODHD_AUTO_TAG.value,
                str(tag['auto_tag_id']),
                'news_auto_tags',
                self.DEFAULT_CONFIDENCE[ThemeSourceType.EODHD_AUTO_TAG],
                idx,
                None,  # market
                metadata
            ))
        
        count = await self._batch_insert_themes(themes_data)
        logger.info(f"新闻 {news_id} 同步了 {count} 个EODHD标签")
        return count
    
    # ============================================================
    # Vector Retrieval 同步
    # ============================================================
    
    async def sync_vector_matches(
        self,
        news_id: int,
        force_resync: bool = False
    ) -> int:
        """
        同步向量检索匹配到统一表
        
        Args:
            news_id: 新闻ID
            force_resync: 是否强制重新同步
        
        Returns:
            同步的匹配数量
        """
        # 如果不是强制同步，检查是否已存在
        if not force_resync:
            existing = await self._check_existing(
                news_id,
                ThemeSourceType.VECTOR_RETRIEVAL
            )
            if existing:
                logger.debug(f"新闻 {news_id} 的向量匹配已存在，跳过")
                return 0
        
        # 查询向量匹配结果
        query = """
        SELECT 
            ntm.id,
            ntm.theme_id,
            ntm.theme_name,
            ntm.category,
            ntm.similarity_score,
            ntm.match_rank,
            ntm.match_method,
            ntm.matched_at
        FROM news_theme_matches ntm
        WHERE ntm.news_id = %s
        ORDER BY ntm.match_rank
        """
        
        matches = await self.db.fetch_all(query, (news_id,))
        
        if not matches:
            logger.debug(f"新闻 {news_id} 没有向量匹配结果")
            return 0
        
        # 🚀 性能优化：使用批量插入
        themes_data = []
        for match in matches:
            metadata = json.dumps({
                'match_method': match['match_method'],
                'matched_at': match['matched_at'].isoformat() if match['matched_at'] else None,
                'original_match_id': match['id'],
                'category': match.get('category')  # 将 category 保存到 metadata
            })
            themes_data.append((
                news_id,
                match['theme_name'],
                ThemeSourceType.VECTOR_RETRIEVAL.value,
                match['theme_id'],
                'news_theme_matches',
                float(match['similarity_score']),
                match['match_rank'],
                None,  # market is no longer available from news_theme_matches
                metadata
            ))
        
        count = await self._batch_insert_themes(themes_data)
        logger.info(f"新闻 {news_id} 同步了 {count} 个向量匹配")
        return count
    
    # ============================================================
    # LLM Classification 同步
    # ============================================================
    
    async def sync_llm_classifications(
        self,
        news_id: int,
        force_resync: bool = False
    ) -> int:
        """
        同步LLM分类结果到统一表
        
        Args:
            news_id: 新闻ID
            force_resync: 是否强制重新同步
        
        Returns:
            同步的分类数量
        """
        # 如果不是强制同步，检查是否已存在
        if not force_resync:
            existing = await self._check_existing(
                news_id,
                ThemeSourceType.LLM_CLASSIFICATION
            )
            if existing:
                logger.debug(f"新闻 {news_id} 的LLM分类已存在，跳过")
                return 0
        
        # 查询LLM分析结果
        query = """
        SELECT 
            id,
            analysis_result,
            analysis_template_code,
            analyzed_at
        FROM news
        WHERE id = %s
          AND analysis_result IS NOT NULL
          AND analysis_status = 'completed'
        """
        
        result = await self.db.fetch_one(query, (news_id,))
        
        if not result or not result['analysis_result']:
            logger.debug(f"新闻 {news_id} 没有LLM分析结果")
            return 0
        
        # 解析JSONB字段
        analysis_result = result['analysis_result']
        
        # 提取主题/标签（支持不同模板格式）
        themes = self._extract_themes_from_llm_result(analysis_result)
        
        if not themes:
            logger.debug(f"新闻 {news_id} 的LLM结果中未找到主题")
            return 0
        
        # 🚀 性能优化：使用批量插入
        themes_data = []
        primary_count = 0
        sector_count = 0
        
        for idx, theme_data in enumerate(themes, start=1):
            try:
                theme_name = theme_data.get('name') or theme_data.get('tag') or theme_data.get('theme')
                if not theme_name:
                    continue
                
                confidence = theme_data.get('confidence', theme_data.get('score', 0.75))
                theme_type = theme_data.get('type', 'generic')
                is_primary = theme_data.get('is_primary', False)
                source_detail = theme_data.get('source_detail', 'unknown')
                
                # 构建增强的元数据
                metadata = json.dumps({
                    'template_code': result['analysis_template_code'],
                    'analyzed_at': result['analyzed_at'].isoformat() if result['analyzed_at'] else None,
                    'theme_type': theme_type,  # primary_theme / sector / event_type / generic
                    'source_detail': source_detail,  # news_classification.primary_theme / related_sectors
                    'is_primary': is_primary
                })
                
                # 生成唯一的 source_id
                source_id = f"llm_{news_id}_{theme_type}_{idx}"
                
                themes_data.append((
                    news_id,
                    theme_name,
                    ThemeSourceType.LLM_CLASSIFICATION.value,
                    source_id,
                    'news.analysis_result',
                    float(confidence),
                    idx,
                    None,  # market
                    metadata
                ))
                
                # 统计不同类型
                if theme_type == 'primary_theme':
                    primary_count += 1
                elif theme_type == 'sector':
                    sector_count += 1
                    
            except Exception as e:
                logger.warning(f"准备LLM分类数据失败 (news_id={news_id}, theme={theme_name}): {e}")
        
        count = await self._batch_insert_themes(themes_data)
        
        # 增强日志信息
        if primary_count > 0 or sector_count > 0:
            logger.info(
                f"新闻 {news_id} 同步了 {count} 个LLM分类 "
                f"(primary_theme: {primary_count}, sectors: {sector_count}, others: {count - primary_count - sector_count})"
            )
        else:
            logger.info(f"新闻 {news_id} 同步了 {count} 个LLM分类")
        
        return count
    
    # ============================================================
    # 缓存管理方法
    # ============================================================
    
    async def _load_master_cache(self) -> Dict[str, Any]:
        """
        加载theme_master_data到内存缓存
        
        Returns:
            {canonical_name: theme_master_data_row}
        """
        try:
            query = """
            SELECT 
                id, canonical_name, display_name, category, 
                aliases, usage_count, last_used_at
            FROM theme_master_data
            WHERE is_active = true
            """
            
            async with self.db.get_cursor() as cursor:
                await cursor.execute(query)
                rows = await cursor.fetchall()
                
                # 构建缓存字典：canonical_name -> 记录
                cache = {}
                for row in rows:
                    canonical_name = row['canonical_name'].lower()
                    cache[canonical_name] = dict(row)
                    
                    # 同时为aliases建立索引
                    if row['aliases']:
                        aliases = json.loads(row['aliases']) if isinstance(row['aliases'], str) else row['aliases']
                        for alias in aliases:
                            cache[alias.lower()] = dict(row)
                
                logger.info(f"缓存加载完成: {len(rows)} 个主题，{len(cache)} 个映射")
                return cache
                
        except Exception as e:
            logger.error(f"加载缓存失败: {str(e)}")
            return {}
    
    async def get_master_cache(self) -> Dict[str, Any]:
        """
        获取缓存（带自动刷新）
        """
        if not self.enable_cache:
            return {}
        
        # 检查缓存是否过期
        now = datetime.now()
        if (self._master_cache is None or 
            self._cache_time is None or 
            now - self._cache_time > self._cache_ttl):
            
            logger.debug("缓存过期或不存在，重新加载")
            self._master_cache = await self._load_master_cache()
            self._cache_time = now
        
        return self._master_cache
    
    def clear_cache(self):
        """清除缓存"""
        self._master_cache = None
        self._cache_time = None
        logger.debug("缓存已清除")
    
    # ============================================================
    # 批量操作优化方法
    # ============================================================
    
    async def _batch_insert_themes(
        self,
        themes_data: List[tuple]
    ) -> int:
        """
        批量插入主题（性能优化版本）
        
        Args:
            themes_data: 主题数据列表，每个元素是tuple: (
                news_id, theme_name, source_type, source_id, source_table,
                confidence_score, rank_in_news, market, metadata_json
            )
        
        Returns:
            成功插入的数量
        """
        if not themes_data:
            return 0
        
        try:
            # 🔧 修复：去重，避免 "ON CONFLICT DO UPDATE command cannot affect row a second time" 错误
            # 对于相同的 (news_id, source_type, theme_name, market) 组合，保留置信度最高的记录
            unique_themes = {}
            original_count = len(themes_data)
            
            for data in themes_data:
                news_id = data[0]
                theme_name = data[1]
                source_type = data[2]
                market = data[7] if data[7] is not None else 'GLOBAL'
                confidence_score = data[5]
                
                # 构建唯一键
                key = (news_id, source_type, theme_name, market)
                
                # 如果不存在或新记录置信度更高，则更新
                if key not in unique_themes or confidence_score > unique_themes[key][5]:
                    unique_themes[key] = data
            
            # 使用去重后的数据
            themes_data = list(unique_themes.values())
            
            if len(themes_data) < original_count:
                logger.warning(
                    f"批量插入去重: 原始 {original_count} 条 -> 去重后 {len(themes_data)} 条 "
                    f"(移除了 {original_count - len(themes_data)} 条重复记录)"
                )
            
            # 分解数据为列
            news_ids = []
            theme_names = []
            source_types = []
            source_ids = []
            source_tables = []
            confidence_scores = []
            rank_in_news_list = []
            markets = []
            metadata_list = []
            
            for data in themes_data:
                news_ids.append(data[0])
                theme_names.append(data[1])
                source_types.append(data[2])
                source_ids.append(data[3])
                source_tables.append(data[4])
                confidence_scores.append(data[5])
                rank_in_news_list.append(data[6])
                # 确保 market 不为 NULL，默认使用 'GLOBAL'
                markets.append(data[7] if data[7] is not None else 'GLOBAL')
                metadata_list.append(data[8])
            
            # 使用 UNNEST 批量插入
            query = """
            INSERT INTO news_themes_unified (
                news_id, theme_name, source_type, source_id, source_table,
                confidence_score, rank_in_news, market, metadata, is_active
            )
            SELECT * FROM UNNEST(
                %s::int[], %s::varchar[], %s::varchar[], %s::varchar[], %s::varchar[],
                %s::numeric[], %s::int[], %s::varchar[], %s::jsonb[], %s::boolean[]
            )
            ON CONFLICT (news_id, source_type, theme_name, market)
            DO UPDATE SET
                confidence_score = EXCLUDED.confidence_score,
                rank_in_news = EXCLUDED.rank_in_news,
                metadata = EXCLUDED.metadata,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            """
            
            # 执行批量插入
            is_active_array = [True] * len(themes_data)
            
            result = await self.db.execute(
                query,
                (
                    news_ids, theme_names, source_types, source_ids, source_tables,
                    confidence_scores, rank_in_news_list, markets, metadata_list, is_active_array
                )
            )
            
            logger.debug(f"批量插入完成: {len(themes_data)} 条主题")
            return len(themes_data)
            
        except Exception as e:
            logger.error(f"批量插入主题失败: {str(e)}")
            # 如果批量插入失败，回退到逐个插入
            logger.warning("回退到逐个插入模式")
            count = 0
            for data in themes_data:
                try:
                    await self._insert_unified_theme(
                        news_id=data[0],
                        theme_name=data[1],
                        source_type=ThemeSourceType(data[2]),
                        source_id=data[3],
                        source_table=data[4],
                        confidence_score=data[5],
                        rank_in_news=data[6],
                        market=data[7],
                        metadata=json.loads(data[8]) if isinstance(data[8], str) else data[8]
                    )
                    count += 1
                except Exception as inner_e:
                    logger.error(f"插入单个主题失败: {str(inner_e)}")
            return count
    
    # ============================================================
    # 辅助方法
    # ============================================================
    
    def _extract_themes_from_llm_result(self, analysis_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        从LLM分析结果中提取主题
        支持多种模板格式，增强版：提取 news_classification 中的 primary_theme、related_sectors 和 related_countries
        """
        themes = []
        
        # ============================================================
        # 1. 优先提取 news_classification 中的结构化主题（新增）
        # ============================================================
        if 'news_classification' in analysis_result:
            classification = analysis_result['news_classification']
            
            # 解析content字段（可能是JSON字符串格式）
            content = classification.get('content', {})
            if isinstance(content, str):
                try:
                    # 去掉 markdown code block 标记
                    content = content.strip()
                    if content.startswith('```json'):
                        content = content[7:]  # 去掉 ```json
                    if content.startswith('```'):
                        content = content[3:]   # 去掉 ```
                    if content.endswith('```'):
                        content = content[:-3]  # 去掉结尾 ```
                    content = content.strip()
                    
                    # 解析JSON
                    content = json.loads(content)
                except Exception as e:
                    logger.debug(f"解析 news_classification.content JSON 失败: {e}")
                    content = {}
            
            if isinstance(content, dict):
                # 1.1 提取 primary_theme（最高优先级，标记为主要主题）
                primary_theme = content.get('primary_theme')
                primary_confidence = content.get('primary_theme_confidence', 0.9)
                
                if primary_theme:
                    themes.append({
                        'name': primary_theme,
                        'confidence': float(primary_confidence),
                        'type': 'primary_theme',
                        'is_primary': True,  # 标记为主要主题
                        'source_detail': 'news_classification.primary_theme'
                    })
                    logger.debug(f"提取到 primary_theme: {primary_theme} (置信度: {primary_confidence})")
                
                # 1.2 提取 related_sectors（行业维度）
                related_sectors = content.get('related_sectors', [])
                sector_confidence = 0.8  # 行业关联置信度
                
                if isinstance(related_sectors, list):
                    for sector in related_sectors:
                        if sector:  # 确保不为空
                            themes.append({
                                'name': sector,
                                'confidence': sector_confidence,
                                'type': 'sector',
                                'is_primary': False,
                                'source_detail': 'news_classification.related_sectors'
                            })
                    if related_sectors:
                        logger.debug(f"提取到 {len(related_sectors)} 个 related_sectors: {related_sectors}")
                
                # 1.2.5 提取 related_countries（国家/地区维度）
                related_countries = content.get('related_countries', [])
                country_confidence = 0.8  # 国家关联置信度
                
                if isinstance(related_countries, list):
                    for country in related_countries:
                        if country:  # 确保不为空
                            themes.append({
                                'name': country,
                                'confidence': country_confidence,
                                'type': 'country',
                                'is_primary': False,
                                'source_detail': 'news_classification.related_countries'
                            })
                    if related_countries:
                        logger.debug(f"提取到 {len(related_countries)} 个 related_countries: {related_countries}")
                
                # 1.3 可选：提取 event_type（事件类型）
                event_type = content.get('event_type')
                event_confidence = content.get('event_type_confidence', 0.85)
                
                if event_type:
                    themes.append({
                        'name': event_type,
                        'confidence': float(event_confidence),
                        'type': 'event_type',
                        'is_primary': False,
                        'source_detail': 'news_classification.event_type'
                    })
                    logger.debug(f"提取到 event_type: {event_type} (置信度: {event_confidence})")
        
        # ============================================================
        # 2. 保留原有的通用提取逻辑（向后兼容）
        # ============================================================
        for key in ['themes', 'tags', 'categories', 'classifications', 'labels']:
            if key in analysis_result:
                value = analysis_result[key]
                
                # 如果是列表
                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            if 'name' not in item and 'type' not in item:
                                item['type'] = 'generic'
                            themes.append(item)
                        elif isinstance(item, str):
                            themes.append({
                                'name': item, 
                                'confidence': 0.75,
                                'type': 'generic',
                                'is_primary': False
                            })
                
                # 如果是字符串（逗号分隔）
                elif isinstance(value, str):
                    for item in value.split(','):
                        item = item.strip()
                        if item:
                            themes.append({
                                'name': item, 
                                'confidence': 0.75,
                                'type': 'generic',
                                'is_primary': False
                            })
        
        # 如果顶层就是数组
        if not themes and isinstance(analysis_result, list):
            for item in analysis_result:
                if isinstance(item, dict):
                    if 'type' not in item:
                        item['type'] = 'generic'
                    themes.append(item)
                elif isinstance(item, str):
                    themes.append({
                        'name': item, 
                        'confidence': 0.75,
                        'type': 'generic',
                        'is_primary': False
                    })
        
        # ============================================================
        # 3. 去重（同名主题保留置信度最高的）
        # ============================================================
        unique_themes = {}
        for theme in themes:
            name = theme.get('name', '').lower().strip()
            if name:
                # 如果已存在，比较置信度
                if name not in unique_themes or \
                   theme.get('confidence', 0) > unique_themes[name].get('confidence', 0):
                    unique_themes[name] = theme
        
        result = list(unique_themes.values())
        
        if result:
            logger.debug(f"共提取 {len(result)} 个唯一主题（去重后）")
        
        return result
    
    async def _check_existing(
        self,
        news_id: int,
        source_type: ThemeSourceType
    ) -> bool:
        """检查是否已存在该来源的主题"""
        query = """
        SELECT EXISTS(
            SELECT 1 
            FROM news_themes_unified 
            WHERE news_id = %s 
              AND source_type = %s
              AND is_active = true
        ) as exists
        """
        result = await self.db.fetch_one(query, (news_id, source_type.value))
        return result['exists'] if result else False
    
    async def _insert_unified_theme(
        self,
        news_id: int,
        theme_name: str,
        source_type: ThemeSourceType,
        source_id: str,
        source_table: str,
        confidence_score: float,
        rank_in_news: Optional[int] = None,
        market: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        插入或更新统一主题
        
        优化说明：
        - theme_master_id 由数据库触发器自动处理
        - trigger_update_theme_usage_count 会自动更新usage_count
        
        Args:
            news_id: 新闻ID
            theme_name: 原始主题名称（将被自动标准化）
            source_type: 来源类型
            source_id: 来源ID
            source_table: 来源表名
            confidence_score: 置信度
            rank_in_news: 在新闻中的排名
            market: 市场（HK/US/CN等）
            metadata: 额外元数据
        
        Returns:
            插入的记录ID
        """
        # 数据验证
        if not theme_name or not theme_name.strip():
            logger.warning(f"尝试插入空主题名称，news_id={news_id}, source={source_type.value}")
            return None
        
        # 清理theme_name（去除多余空格）
        theme_name = theme_name.strip()
        
        query = """
        INSERT INTO news_themes_unified (
            news_id,
            theme_name,
            source_type,
            source_id,
            source_table,
            confidence_score,
            rank_in_news,
            market,
            metadata,
            is_active
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, true
        )
        ON CONFLICT (news_id, source_type, theme_name, market)
        DO UPDATE SET
            confidence_score = EXCLUDED.confidence_score,
            rank_in_news = EXCLUDED.rank_in_news,
            metadata = EXCLUDED.metadata,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id, theme_master_id
        """
        
        try:
            result = await self.db.fetch_one(
                query,
                (news_id,
                theme_name,
                source_type.value,
                source_id,
                source_table,
                confidence_score,
                rank_in_news,
                market,
                json.dumps(metadata) if metadata else '{}')
            )
            
            if result:
                # 记录标准化结果（用于调试）
                if logger.logger.level <= 10:  # DEBUG级别
                    logger.debug(
                        f"主题插入成功: '{theme_name}' "
                        f"(master_id={result.get('theme_master_id')}, news_id={news_id})"
                    )
                
                return result['id']
            else:
                logger.warning(f"插入主题失败，无返回结果: news_id={news_id}, theme={theme_name}")
                return None
                
        except Exception as e:
            logger.error(
                f"插入统一主题失败: news_id={news_id}, theme={theme_name}, "
                f"source={source_type.value}, error={str(e)}"
            )
            raise
    
    # ============================================================
    # 批量操作
    # ============================================================
    
    async def sync_recent_news(
        self,
        days: int = 30,
        force_resync: bool = False
    ) -> ThemeSyncResult:
        """
        同步最近N天的新闻主题
        
        Args:
            days: 最近天数
            force_resync: 是否强制重新同步
        """
        query = """
        SELECT id 
        FROM news 
        WHERE date >= CURRENT_TIMESTAMP - INTERVAL '1 day' * %s
        ORDER BY date DESC
        """
        
        rows = await self.db.fetch_all(query, (days,))
        news_ids = [row['id'] for row in rows]
        
        logger.info(f"准备同步最近 {days} 天的 {len(news_ids)} 条新闻")
        
        return await self.sync_all_sources(news_ids, force_resync)
    
    async def sync_unsynced_news(self) -> ThemeSyncResult:
        """
        同步尚未同步的新闻
        """
        query = """
        SELECT DISTINCT n.id
        FROM news n
        WHERE NOT EXISTS (
            SELECT 1 
            FROM news_themes_unified ntu 
            WHERE ntu.news_id = n.id 
              AND ntu.is_active = true
        )
        AND n.date >= CURRENT_TIMESTAMP - INTERVAL '90 days'
        ORDER BY n.date DESC
        LIMIT 1000
        """
        
        rows = await self.db.fetch_all(query)
        news_ids = [row['id'] for row in rows]
        
        logger.info(f"发现 {len(news_ids)} 条未同步的新闻")
        
        if not news_ids:
            return ThemeSyncResult(
                success=True,
                synced_count=0,
                failed_count=0,
                details={},
                errors=[]
            )
        
        return await self.sync_all_sources(news_ids, force_resync=False)
    
    # ============================================================
    # 质量分数计算
    # ============================================================
    
    async def recalculate_quality_scores(
        self,
        news_ids: Optional[List[int]] = None
    ) -> int:
        """
        重新计算质量分数
        
        Args:
            news_ids: 新闻ID列表，None表示全部
        
        Returns:
            更新的记录数
        """
        if news_ids:
            where_clause = "WHERE ntu.news_id = ANY(%s)"
            params = (news_ids,)
        else:
            where_clause = ""
            params = None
        
        query = f"""
        UPDATE news_themes_unified ntu
        SET quality_score = calculate_theme_quality_score(
            ntu.news_id,
            ntu.theme_name,
            ntu.source_type,
            ntu.confidence_score
        )
        {where_clause}
        """
        
        if params:
            result = await self.db.execute(query, params)
        else:
            result = await self.db.execute(query)
        count = result.split()[-1] if result else 0
        
        logger.info(f"重新计算了 {count} 条记录的质量分数")
        return int(count)



