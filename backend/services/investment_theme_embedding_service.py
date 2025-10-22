# -*- coding: utf-8 -*-
"""
投资主题向量化服务
用于生成和管理投资主题的Embedding向量，支持基于向量检索的新闻主题匹配
"""

import asyncio
import os
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from backend.database.db_manager import DatabaseManager
from backend.core.llm_framework import llm_service
from backend.utils.logger import get_logger

logger = get_logger("investment_theme_embedding_service")


class InvestmentThemeEmbeddingService:
    """投资主题向量化服务"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        初始化服务
        
        Args:
            db_config: 数据库配置
        """
        self.db_manager = DatabaseManager(db_config)
        self.embedding_model = "text-embedding-v3"
        self.embedding_dimension = 1024
        self.use_pgvector = None  # 自动检测：True=pgvector, False=JSONB
        
        # 🌟 新增：是否使用LLM生成主题描述（从环境变量读取，默认启用）
        self.use_llm_description = os.getenv('USE_LLM_THEME_DESCRIPTION', 'true').lower() == 'true'
        self.llm_template_code = 'theme_description_generator_v1'
        
        logger.info(f"投资主题向量化服务初始化完成 | LLM描述生成: {'启用' if self.use_llm_description else '禁用'}")
    
    async def _generate_enriched_theme_text(self, cursor, theme_id: str, theme_name: str, market: str, db_theme_id: int = None) -> str:
        """
        生成增强版主题文本描述
        🌟 支持两种模式：
        1. LLM模式（默认）：使用Prompt模板调用LLM生成详细描述
        2. 简单模式：使用模板拼接（作为降级方案）
        
        Args:
            cursor: 数据库游标
            theme_id: 主题ID（字符串，如 BK1232）
            theme_name: 主题名称
            market: 市场代码
            db_theme_id: 数据库主题ID（整数）
            
        Returns:
            增强版主题文本描述
        """
        try:
            # 如果没有提供 db_theme_id，先查询
            if db_theme_id is None:
                cursor.execute("""
                    SELECT id FROM futu_investment_themes
                    WHERE theme_id = %s AND market = %s
                    LIMIT 1
                """, (theme_id, market))
                
                result = cursor.fetchone()
                if result:
                    db_theme_id = result[0]
                else:
                    # 找不到主题记录，使用简化版本
                    return f"Investment theme: {theme_name}. Market: {market} market. Investment category focusing on {theme_name.lower()} sector."
            
            # 从 futu_theme_stocks 表获取该主题下的股票列表
            cursor.execute("""
                SELECT DISTINCT symbol, stock_name
                FROM futu_theme_stocks
                WHERE theme_id = %s AND market = %s
                ORDER BY symbol
                LIMIT 20
            """, (db_theme_id, market))
            
            stocks = cursor.fetchall()
            
            # 准备股票信息
            stock_symbols = [s[0] for s in stocks[:10]] if stocks else []
            stock_symbols_str = ', '.join(stock_symbols) if stock_symbols else 'Not available'
            
            stock_names = [s[1] for s in stocks[:5] if s[1]] if stocks else []
            stock_names_str = ', '.join(stock_names) if stock_names else 'Not available'
            
            # 🌟 模式1：使用LLM生成描述（推荐）
            if self.use_llm_description:
                try:
                    logger.debug(f"🤖 使用LLM生成主题描述: {theme_name}")
                    
                    response = await llm_service.chat_completion_with_template(
                        template_code=self.llm_template_code,
                        variables={
                            'theme_name': theme_name,
                            'market': market,
                            'stock_symbols': stock_symbols_str,
                            'company_names': stock_names_str
                        }
                    )
                    
                    description = response.content.strip()
                    
                    # 验证生成的描述不为空
                    if description and len(description) > 50:
                        logger.info(f"✅ LLM生成描述成功: {theme_name} ({len(description)} chars)")
                        return description
                    else:
                        logger.warning(f"LLM生成的描述过短，降级使用简单模板: {theme_name}")
                        
                except Exception as llm_error:
                    logger.warning(f"LLM生成描述失败，降级使用简单模板: {str(llm_error)}")
                    # 降级到简单模板
            
            # 🔧 模式2：简单模板（降级方案）
            if stocks and len(stocks) > 0:
                # 格式1：包含股票代码和公司名称
                text = f"Investment theme: {theme_name}. Market: {market}. Related stocks include: {stock_symbols_str}."
                
                if stock_names:
                    text += f" Companies: {stock_names_str}."
                
                logger.debug(f"使用简单模板: {theme_name} -> {len(stocks)} 个股票")
            else:
                # 格式2：没有股票时的描述
                text = f"Investment theme: {theme_name}. Market: {market} market. Investment category focusing on {theme_name.lower()} sector."
                logger.debug(f"无股票数据，使用简化描述: {theme_name}")
            
            return text
            
        except Exception as e:
            # 如果出错，返回最简化版本
            logger.warning(f"生成增强文本失败，使用最简版本: {str(e)}")
            return f"{theme_name} ({market} market)"
    
    def _detect_database_mode(self, cursor) -> bool:
        """
        检测数据库使用的向量存储模式
        
        Args:
            cursor: 数据库游标
            
        Returns:
            True: 使用pgvector (embedding_vector列)
            False: 使用JSONB (embedding_data列)
        """
        if self.use_pgvector is not None:
            return self.use_pgvector
        
        try:
            # 检查表结构
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'investment_theme_embeddings'
                AND column_name IN ('embedding_vector', 'embedding_data')
            """)
            
            result = cursor.fetchone()
            if result:
                column_name = result[0]
                self.use_pgvector = (column_name == 'embedding_vector')
                logger.info(f"检测到数据库模式: {'pgvector' if self.use_pgvector else 'JSONB'}")
                return self.use_pgvector
            else:
                # 默认使用JSONB模式
                logger.warning("未找到向量列，默认使用JSONB模式")
                self.use_pgvector = False
                return False
                
        except Exception as e:
            logger.error(f"检测数据库模式失败: {str(e)}")
            # 默认使用JSONB模式
            self.use_pgvector = False
            return False
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        计算两个向量的余弦相似度
        
        Args:
            vec1: 向量1
            vec2: 向量2
            
        Returns:
            余弦相似度 (0-1)
        """
        try:
            import numpy as np
            
            # 转换为numpy数组
            v1 = np.array(vec1)
            v2 = np.array(vec2)
            
            # 计算余弦相似度
            dot_product = np.dot(v1, v2)
            norm_v1 = np.linalg.norm(v1)
            norm_v2 = np.linalg.norm(v2)
            
            if norm_v1 == 0 or norm_v2 == 0:
                return 0.0
            
            similarity = dot_product / (norm_v1 * norm_v2)
            
            # 确保结果在 [0, 1] 范围内
            return max(0.0, min(1.0, float(similarity)))
            
        except Exception as e:
            logger.error(f"计算余弦相似度失败: {str(e)}")
            return 0.0
    
    async def initialize_all_theme_embeddings(self, force_update: bool = False) -> Dict[str, Any]:
        """
        初始化所有投资主题的向量
        从 futu_investment_themes 表读取主题，生成并存储向量
        
        Args:
            force_update: 是否强制更新已存在的向量
            
        Returns:
            处理统计信息
        """
        logger.info("开始初始化投资主题向量", force_update=force_update)
        
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("数据库连接失败")
            return {"success": False, "error": "数据库连接失败"}
        
        try:
            cursor = conn.cursor()
            
            # 1. 获取所有Futu主题（去重，同时获取数据库ID）
            cursor.execute("""
                SELECT DISTINCT id, theme_id, theme_name, market
                FROM futu_investment_themes
                ORDER BY market, theme_name
            """)
            
            themes = cursor.fetchall()
            total_themes = len(themes)
            
            if total_themes == 0:
                logger.warning("未找到任何投资主题")
                return {"success": True, "message": "没有需要处理的主题", "total": 0}
            
            logger.info(f"找到 {total_themes} 个投资主题")
            
            # 2. 检查哪些主题需要生成向量
            themes_to_process = []
            
            if force_update:
                themes_to_process = themes
                logger.info(f"强制更新模式，将处理所有 {total_themes} 个主题")
            else:
                # 只处理未生成向量的主题
                for theme in themes:
                    theme_id = theme[1]  # 注意：现在是第2个字段（theme_id字符串）
                    cursor.execute("""
                        SELECT id FROM investment_theme_embeddings 
                        WHERE theme_id = %s
                    """, (theme_id,))
                    
                    if not cursor.fetchone():
                        themes_to_process.append(theme)
                
                logger.info(f"发现 {len(themes_to_process)} 个新主题需要生成向量")
            
            if len(themes_to_process) == 0:
                logger.info("所有主题向量已存在，无需更新")
                return {
                    "success": True,
                    "message": "所有主题向量已存在",
                    "total": total_themes,
                    "processed": 0,
                    "skipped": total_themes
                }
            
            # 3. 检测数据库模式
            use_pgvector = self._detect_database_mode(cursor)
            
            # 4. 批量生成向量（每次10个，Qwen API限制）
            batch_size = 10  # Qwen API要求批次大小不超过10
            processed_count = 0
            failed_count = 0
            
            for i in range(0, len(themes_to_process), batch_size):
                batch = themes_to_process[i:i+batch_size]
                # 现在每个 theme 是 (id, theme_id, theme_name, market)
                batch_db_ids = [theme[0] for theme in batch]
                batch_theme_ids = [theme[1] for theme in batch]
                batch_theme_names = [theme[2] for theme in batch]
                batch_markets = [theme[3] for theme in batch]
                
                # 🔥 优化：构建增强版文本描述（包含股票信息）
                batch_texts = []
                for db_id, theme_id, theme_name, market in zip(batch_db_ids, batch_theme_ids, batch_theme_names, batch_markets):
                    # 使用新的增强文本生成方法，传入数据库ID
                    text = await self._generate_enriched_theme_text(cursor, theme_id, theme_name, market, db_id)
                    batch_texts.append(text)
                
                logger.info(f"处理批次 {i//batch_size + 1}/{(len(themes_to_process) + batch_size - 1)//batch_size}")
                
                # 显示第一个批次的示例文本（用于验证）
                if i == 0 and len(batch_texts) > 0:
                    logger.info(f"📝 增强文本示例: {batch_texts[0][:150]}...")
                
                try:
                    # 调用Qwen Embedding API
                    if 'qwen' not in llm_service.providers:
                        raise Exception("Qwen提供商未配置，请检查LLM配置")
                    
                    embeddings = await llm_service.providers['qwen'].generate_embeddings(
                        texts=batch_texts,
                        model=self.embedding_model,
                        text_type="document"
                    )
                    
                    # 5. 存储向量到数据库
                    for j in range(len(batch)):
                        if j >= len(embeddings):
                            logger.warning(f"主题 {batch_theme_ids[j]} 未获得向量")
                            failed_count += 1
                            continue
                        
                        theme_id = batch_theme_ids[j]
                        theme_name = batch_theme_names[j]
                        market = batch_markets[j]
                        embedding = embeddings[j]
                        
                        # 验证向量维度
                        if len(embedding) != self.embedding_dimension:
                            logger.error(
                                f"向量维度不匹配: 期望{self.embedding_dimension}, 实际{len(embedding)}"
                            )
                            failed_count += 1
                            continue
                        
                        # 根据数据库模式选择不同的插入语句
                        if use_pgvector:
                            # pgvector模式：使用 embedding_vector 列
                            cursor.execute("""
                                INSERT INTO investment_theme_embeddings 
                                (theme_id, theme_name, market, embedding_vector, embedding_model)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (theme_id) 
                                DO UPDATE SET 
                                    embedding_vector = EXCLUDED.embedding_vector,
                                    embedding_model = EXCLUDED.embedding_model,
                                    updated_at = NOW()
                            """, (theme_id, theme_name, market, embedding, self.embedding_model))
                        else:
                            # JSONB模式：使用 embedding_data 列
                            import json
                            embedding_json = json.dumps(embedding)
                            cursor.execute("""
                                INSERT INTO investment_theme_embeddings 
                                (theme_id, theme_name, market, embedding_data, embedding_model)
                                VALUES (%s, %s, %s, %s::jsonb, %s)
                                ON CONFLICT (theme_id) 
                                DO UPDATE SET 
                                    embedding_data = EXCLUDED.embedding_data,
                                    embedding_model = EXCLUDED.embedding_model,
                                    updated_at = NOW()
                            """, (theme_id, theme_name, market, embedding_json, self.embedding_model))
                        
                        processed_count += 1
                    
                    conn.commit()
                    logger.info(f"批次处理完成，已处理 {processed_count}/{len(themes_to_process)} 个主题")
                    
                    # 避免API限流，添加延迟
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"批次处理失败: {str(e)}")
                    failed_count += len(batch)
                    conn.rollback()
                    continue
            
            cursor.close()
            conn.close()
            
            result = {
                "success": True,
                "total": total_themes,
                "processed": processed_count,
                "failed": failed_count,
                "skipped": total_themes - len(themes_to_process)
            }
            
            logger.info(
                f"投资主题向量初始化完成: "
                f"总数{total_themes}, 处理{processed_count}, 失败{failed_count}, 跳过{result['skipped']}"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"初始化投资主题向量失败: {str(e)}")
            if conn:
                conn.close()
            return {"success": False, "error": str(e)}
    
    async def match_news_to_themes(
        self, 
        news_text: str,
        news_id: Optional[int] = None,
        market: Optional[str] = None,
        top_k: int = 5,
        min_similarity: float = 0.60,
        save_to_db: bool = False
    ) -> List[Dict[str, Any]]:
        """
        将新闻匹配到最相关的投资主题
        
        Args:
            news_text: 新闻文本（建议使用标题+摘要）
            news_id: 新闻ID（用于保存匹配结果）
            market: 市场过滤（HK/US/CN），None表示不过滤
            top_k: 返回Top-K个主题
            min_similarity: 最小相似度阈值（0-1）
            save_to_db: 是否保存匹配结果到数据库
            
        Returns:
            匹配的主题列表，按相似度降序排列
            
        Example:
            matches = await service.match_news_to_themes(
                news_text="Tesla发布新款电动车",
                market="US",
                top_k=3,
                min_similarity=0.6
            )
        """
        if not news_text or len(news_text.strip()) == 0:
            logger.warning("新闻文本为空，无法匹配主题")
            return []
        
        try:
            # 1. 生成新闻向量
            if 'qwen' not in llm_service.providers:
                raise Exception("Qwen提供商未配置")
            
            logger.info(f"生成新闻向量: {news_text[:50]}...")
            
            embeddings = await llm_service.providers['qwen'].generate_embeddings(
                texts=[news_text],
                model=self.embedding_model,
                text_type="query"  # 查询模式
            )
            
            if not embeddings or len(embeddings) == 0:
                logger.error("生成新闻向量失败")
                return []
            
            news_vector = embeddings[0]
            
            # 2. 向量相似度检索
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("数据库连接失败")
                return []
            
            cursor = conn.cursor()
            
            # 检测数据库模式
            use_pgvector = self._detect_database_mode(cursor)
            
            # 根据数据库模式选择不同的查询方式
            if use_pgvector:
                # pgvector模式：使用向量操作符
                market_filter = ""
                params = [news_vector, news_vector, top_k]
                
                if market:
                    market_filter = "AND market = %s"
                    params.insert(2, market)
                
                # 使用pgvector的余弦相似度操作符 <=>
                # 1 - (v1 <=> v2) = 余弦相似度
                query = f"""
                    SELECT 
                        theme_id,
                        theme_name,
                        market,
                        1 - (embedding_vector <=> %s::vector) as similarity
                    FROM investment_theme_embeddings
                    WHERE 1=1 {market_filter}
                    ORDER BY embedding_vector <=> %s::vector
                    LIMIT %s
                """
                
                cursor.execute(query, params)
            else:
                # JSONB模式：先获取所有主题，然后在Python中计算相似度
                import json
                import numpy as np
                
                market_filter = ""
                params = []
                
                if market:
                    market_filter = "WHERE market = %s"
                    params.append(market)
                
                query = f"""
                    SELECT 
                        theme_id,
                        theme_name,
                        market,
                        embedding_data
                    FROM investment_theme_embeddings
                    {market_filter}
                """
                
                cursor.execute(query, params)
                
                # 在Python中计算余弦相似度
                theme_similarities = []
                for row in cursor.fetchall():
                    theme_id = row[0]
                    theme_name = row[1]
                    theme_market = row[2]
                    embedding_data = row[3]
                    
                    # 解析JSONB向量
                    if isinstance(embedding_data, str):
                        theme_vector = json.loads(embedding_data)
                    else:
                        theme_vector = embedding_data
                    
                    # 计算余弦相似度
                    similarity = self._cosine_similarity(news_vector, theme_vector)
                    
                    theme_similarities.append({
                        'theme_id': theme_id,
                        'theme_name': theme_name,
                        'market': theme_market,
                        'similarity': similarity
                    })
                
                # 按相似度排序
                theme_similarities.sort(key=lambda x: x['similarity'], reverse=True)
            
            results = []
            
            if use_pgvector:
                # 处理pgvector查询结果
                for idx, row in enumerate(cursor.fetchall()):
                    theme_id = row[0]
                    theme_name = row[1]
                    theme_market = row[2]
                    similarity = float(row[3])
                    
                    # 过滤低于阈值的结果
                    if similarity < min_similarity:
                        continue
                    
                    match_result = {
                        'theme_id': theme_id,
                        'theme_name': theme_name,
                        'market': theme_market,
                        'similarity': round(similarity, 4),
                        'confidence': round(similarity, 2),
                        'rank': len(results) + 1  # 使用results长度+1作为排名
                    }
                    
                    results.append(match_result)
                    
                    # 限制结果数量为top_k
                    if len(results) >= top_k:
                        break
            else:
                # 处理JSONB模式的结果
                for theme in theme_similarities:
                    similarity = theme['similarity']
                    
                    # 过滤低于阈值的结果
                    if similarity < min_similarity:
                        continue
                    
                    match_result = {
                        'theme_id': theme['theme_id'],
                        'theme_name': theme['theme_name'],
                        'market': theme['market'],
                        'similarity': round(similarity, 4),
                        'confidence': round(similarity, 2),
                        'rank': len(results) + 1  # 使用results长度+1作为排名
                    }
                    
                    results.append(match_result)
                    
                    # 限制结果数量为top_k
                    if len(results) >= top_k:
                        break
            
            logger.info(f"找到 {len(results)} 个匹配主题（阈值: {min_similarity}）")
            
            # 3. 保存匹配结果到数据库（可选）
            if save_to_db and news_id and len(results) > 0:
                try:
                    for match in results:
                        cursor.execute("""
                            INSERT INTO news_theme_matches 
                            (news_id, theme_id, theme_name, market, similarity_score, match_rank, match_method)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (news_id, theme_id) 
                            DO UPDATE SET 
                                similarity_score = EXCLUDED.similarity_score,
                                match_rank = EXCLUDED.match_rank,
                                matched_at = NOW()
                        """, (
                            news_id,
                            match['theme_id'],
                            match['theme_name'],
                            match['market'],
                            match['similarity'],
                            match['rank'],
                            'vector_search'
                        ))
                    
                    conn.commit()
                    logger.info(f"已保存 {len(results)} 个主题匹配结果到数据库")
                    
                except Exception as e:
                    logger.error(f"保存匹配结果失败: {str(e)}")
                    conn.rollback()
            
            cursor.close()
            conn.close()
            
            return results
            
        except Exception as e:
            logger.error(f"新闻主题匹配失败: {str(e)}")
            return []
    
    def extract_market_from_symbols(self, symbols: List[str]) -> Optional[str]:
        """
        从股票代码中提取市场信息
        
        Args:
            symbols: 股票代码列表（例如：["0700.HK", "AAPL"]）
            
        Returns:
            市场代码（HK/US），如果无法判断则返回None
        """
        if not symbols:
            return None
        
        hk_count = 0
        us_count = 0
        
        for symbol in symbols:
            symbol_upper = symbol.upper()
            
            # 检测香港股票
            if '.HK' in symbol_upper or symbol_upper.endswith('.HK'):
                hk_count += 1
            # 检测美股（通常是纯字母代码）
            elif re.match(r'^[A-Z]{1,5}$', symbol_upper):
                us_count += 1
        
        # 返回主要市场
        if hk_count > us_count:
            return 'HK'
        elif us_count > hk_count:
            return 'US'
        else:
            return None
    
    def get_theme_statistics(self) -> Dict[str, Any]:
        """
        获取主题向量统计信息
        
        Returns:
            统计信息字典
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            
            # 统计各市场的主题数量
            cursor.execute("""
                SELECT 
                    market,
                    COUNT(*) as count,
                    MAX(updated_at) as last_updated
                FROM investment_theme_embeddings
                GROUP BY market
                ORDER BY market
            """)
            
            market_stats = []
            total_count = 0
            
            for row in cursor.fetchall():
                market = row[0]
                count = row[1]
                last_updated = row[2]
                
                market_stats.append({
                    'market': market,
                    'count': count,
                    'last_updated': last_updated.isoformat() if last_updated else None
                })
                
                total_count += count
            
            # 统计新闻主题匹配数量
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT news_id) as news_with_themes,
                    COUNT(*) as total_matches,
                    AVG(similarity_score) as avg_similarity
                FROM news_theme_matches
            """)
            
            match_row = cursor.fetchone()
            match_stats = {
                'news_with_themes': match_row[0] if match_row else 0,
                'total_matches': match_row[1] if match_row else 0,
                'avg_similarity': float(match_row[2]) if match_row and match_row[2] else 0.0
            }
            
            cursor.close()
            conn.close()
            
            return {
                'total_themes': total_count,
                'market_breakdown': market_stats,
                'match_statistics': match_stats,
                'embedding_model': self.embedding_model,
                'embedding_dimension': self.embedding_dimension
            }
            
        except Exception as e:
            logger.error(f"获取主题统计信息失败: {str(e)}")
            if conn:
                conn.close()
            return {}
    
    async def update_single_theme_embedding(
        self, 
        theme_id: str,
        theme_name: str,
        market: str
    ) -> bool:
        """
        更新单个主题的向量
        
        Args:
            theme_id: 主题ID
            theme_name: 主题名称
            market: 市场代码
            
        Returns:
            是否成功
        """
        try:
            # 生成文本描述
            text = f"{theme_name} ({market} market)"
            
            # 生成向量
            if 'qwen' not in llm_service.providers:
                raise Exception("Qwen提供商未配置")
            
            embeddings = await llm_service.providers['qwen'].generate_embeddings(
                texts=[text],
                model=self.embedding_model,
                text_type="document"
            )
            
            if not embeddings or len(embeddings) == 0:
                logger.error(f"生成主题向量失败: {theme_id}")
                return False
            
            embedding = embeddings[0]
            
            # 存储到数据库
            conn = self.db_manager.get_connection()
            if not conn:
                return False
            
            cursor = conn.cursor()
            
            # 检测数据库模式
            use_pgvector = self._detect_database_mode(cursor)
            
            # 根据数据库模式选择不同的插入语句
            if use_pgvector:
                # pgvector模式
                cursor.execute("""
                    INSERT INTO investment_theme_embeddings 
                    (theme_id, theme_name, market, embedding_vector, embedding_model)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (theme_id) 
                    DO UPDATE SET 
                        theme_name = EXCLUDED.theme_name,
                        market = EXCLUDED.market,
                        embedding_vector = EXCLUDED.embedding_vector,
                        embedding_model = EXCLUDED.embedding_model,
                        updated_at = NOW()
                """, (theme_id, theme_name, market, embedding, self.embedding_model))
            else:
                # JSONB模式
                import json
                embedding_json = json.dumps(embedding)
                cursor.execute("""
                    INSERT INTO investment_theme_embeddings 
                    (theme_id, theme_name, market, embedding_data, embedding_model)
                    VALUES (%s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (theme_id) 
                    DO UPDATE SET 
                        theme_name = EXCLUDED.theme_name,
                        market = EXCLUDED.market,
                        embedding_data = EXCLUDED.embedding_data,
                        embedding_model = EXCLUDED.embedding_model,
                        updated_at = NOW()
                """, (theme_id, theme_name, market, embedding_json, self.embedding_model))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"更新主题向量成功: {theme_id} - {theme_name}")
            return True
            
        except Exception as e:
            logger.error(f"更新主题向量失败: {str(e)}")
            return False

