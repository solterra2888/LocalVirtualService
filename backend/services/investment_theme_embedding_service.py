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
from backend.core.llm_framework import llm_service, LLMMessage
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
    
    async def _generate_enriched_theme_text(
        self, 
        cursor, 
        theme_id: str, 
        theme_name: str, 
        market: str, 
        db_theme_id: int = None,
        force_regenerate: bool = False
    ) -> str:
        """
        生成增强版主题文本描述
        
        🌟 优化后的缓存策略：
        1. 优先使用数据库中已缓存的 description
        2. 如果缓存不存在或 force_regenerate=True，则调用 LLM 生成
        3. 生成成功后保存到数据库缓存
        4. LLM 失败时降级使用简单模板
        
        Args:
            cursor: 数据库游标
            theme_id: 主题ID（字符串，如 BK1232）
            theme_name: 主题名称
            market: 市场代码
            db_theme_id: 数据库主题ID（整数）
            force_regenerate: 是否强制重新生成（忽略缓存）
            
        Returns:
            增强版主题文本描述
        """
        try:
            # 步骤1: 获取数据库主题ID和检查缓存的描述
            if db_theme_id is None:
                cursor.execute("""
                    SELECT id, description, description_source 
                    FROM futu_investment_themes
                    WHERE theme_id = %s AND market = %s
                    LIMIT 1
                """, (theme_id, market))
                
                result = cursor.fetchone()
                if result:
                    db_theme_id, cached_description, description_source = result
                else:
                    # 找不到主题记录，使用简化版本
                    logger.warning(f"主题不存在: {theme_id} ({market})")
                    return f"Investment theme: {theme_name}. Market: {market} market. Investment category focusing on {theme_name.lower()} sector."
            else:
                # 如果提供了 db_theme_id，查询缓存的描述
                cursor.execute("""
                    SELECT description, description_source
                    FROM futu_investment_themes
                    WHERE id = %s
                """, (db_theme_id,))
                
                result = cursor.fetchone()
                cached_description, description_source = result if result else (None, None)
            
            # 步骤2: 如果有缓存的描述且不强制重新生成，直接返回
            if cached_description and not force_regenerate:
                logger.debug(f"✅ 使用缓存描述: {theme_name} (来源: {description_source or 'unknown'})")
                return cached_description
            
            # 步骤3: 获取股票信息（用于生成描述）
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
            
            generated_description = None
            description_source_new = None
            
            # 步骤4: 尝试使用 LLM 生成描述
            if self.use_llm_description:
                try:
                    logger.debug(f"🤖 调用LLM生成描述: {theme_name}")
                    
                    response = await llm_service.chat_completion_with_template(
                        template_code=self.llm_template_code,
                        variables={
                            'theme_name': theme_name,
                            'market': market,
                            'stock_symbols': stock_symbols_str,
                            'company_names': stock_names_str
                        }
                    )
                    
                    generated_description = response.content.strip()
                    
                    # 验证生成的描述
                    if generated_description and len(generated_description) > 50:
                        description_source_new = 'llm_generated'
                        logger.info(f"✅ LLM生成成功: {theme_name} ({len(generated_description)} chars)")
                    else:
                        logger.warning(f"LLM生成的描述过短: {theme_name}")
                        generated_description = None
                        
                except Exception as llm_error:
                    logger.warning(f"LLM生成失败: {theme_name} - {str(llm_error)}")
                    generated_description = None
            
            # 步骤5: 如果 LLM 生成失败，使用简单模板
            if not generated_description:
                if stocks and len(stocks) > 0:
                    generated_description = f"Investment theme: {theme_name}. Market: {market}. Related stocks include: {stock_symbols_str}."
                    if stock_names:
                        generated_description += f" Companies: {stock_names_str}."
                    logger.debug(f"使用简单模板: {theme_name} ({len(stocks)} stocks)")
                else:
                    generated_description = f"Investment theme: {theme_name}. Market: {market} market. Investment category focusing on {theme_name.lower()} sector."
                    logger.debug(f"使用最简模板: {theme_name} (无股票数据)")
                
                description_source_new = 'simple_template'
            
            # 步骤6: 保存生成的描述到数据库缓存
            if generated_description and db_theme_id:
                try:
                    cursor.execute("""
                        UPDATE futu_investment_themes
                        SET 
                            description = %s,
                            description_source = %s,
                            description_updated_at = NOW()
                        WHERE id = %s
                    """, (generated_description, description_source_new, db_theme_id))
                    
                    # 提交更新（如果在事务中）
                    if cursor.connection:
                        cursor.connection.commit()
                    
                    logger.debug(f"💾 描述已缓存: {theme_name} (来源: {description_source_new})")
                    
                except Exception as db_error:
                    logger.warning(f"保存描述到数据库失败: {theme_name} - {str(db_error)}")
                    # 即使保存失败，也返回生成的描述
            
            return generated_description
            
        except Exception as e:
            # 如果出错，返回最简化版本
            logger.error(f"生成主题描述失败: {theme_name} - {str(e)}")
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
                # 🌟 支持缓存：如果 force_update=True，强制重新生成描述
                # 🚀 使用并发处理加速描述生成
                description_tasks = []
                for db_id, theme_id, theme_name, market in zip(batch_db_ids, batch_theme_ids, batch_theme_names, batch_markets):
                    # 使用新的增强文本生成方法，传入数据库ID
                    # force_regenerate=force_update 确保强制更新时重新生成描述
                    task = self._generate_enriched_theme_text(
                        cursor, theme_id, theme_name, market, db_id, 
                        force_regenerate=force_update
                    )
                    description_tasks.append(task)
                
                # 并发执行所有描述生成任务
                batch_texts = await asyncio.gather(*description_tasks, return_exceptions=True)
                
                # 处理异常，将异常结果替换为简单描述
                for idx, text in enumerate(batch_texts):
                    if isinstance(text, Exception):
                        theme_name = batch_theme_names[idx]
                        market = batch_markets[idx]
                        logger.warning(f"描述生成失败，使用简单描述: {theme_name} - {str(text)}")
                        batch_texts[idx] = f"{theme_name} ({market} market)"
                
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
                # JOIN futu_investment_themes来过滤启用且未删除的主题
                market_filter = ""
                params = [news_vector, news_vector, top_k]
                
                if market:
                    market_filter = "AND e.market = %s"
                    params.insert(2, market)
                
                # 使用pgvector的余弦相似度操作符 <=>
                # 1 - (v1 <=> v2) = 余弦相似度
                # JOIN主表过滤：is_deleted = FALSE AND is_enabled_for_matching = TRUE
                query = f"""
                    SELECT 
                        e.theme_id,
                        e.theme_name,
                        e.market,
                        1 - (e.embedding_vector <=> %s::vector) as similarity
                    FROM investment_theme_embeddings e
                    INNER JOIN futu_investment_themes t 
                        ON e.theme_id = t.theme_id AND e.market = t.market
                    WHERE t.is_deleted = FALSE 
                        AND t.is_enabled_for_matching = TRUE
                        {market_filter}
                    ORDER BY e.embedding_vector <=> %s::vector
                    LIMIT %s
                """
                
                cursor.execute(query, params)
            else:
                # JSONB模式：先获取所有主题，然后在Python中计算相似度
                # JOIN主表过滤：is_deleted = FALSE AND is_enabled_for_matching = TRUE
                import json
                import numpy as np
                
                market_filter = ""
                params = []
                
                if market:
                    market_filter = "AND e.market = %s"
                    params.append(market)
                
                query = f"""
                    SELECT 
                        e.theme_id,
                        e.theme_name,
                        e.market,
                        e.embedding_data
                    FROM investment_theme_embeddings e
                    INNER JOIN futu_investment_themes t 
                        ON e.theme_id = t.theme_id AND e.market = t.market
                    WHERE t.is_deleted = FALSE 
                        AND t.is_enabled_for_matching = TRUE
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
    
    async def update_theme_description(
        self,
        theme_id: str,
        market: str,
        new_description: str,
        editor_username: str,
        version: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        更新主题描述并重新生成向量
        
        Args:
            theme_id: 主题ID
            market: 市场代码
            new_description: 新的描述内容
            editor_username: 编辑者用户名
            version: 乐观锁版本号（可选，用于并发控制）
            
        Returns:
            {
                "success": bool,
                "message": str,
                "description_saved": bool,  # 描述是否保存成功
                "embedding_updated": bool,  # 向量是否更新成功
                "error": str (optional)
            }
        """
        logger.info(f"开始更新主题描述: {theme_id} ({market})")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return {
                "success": False,
                "message": "数据库连接失败",
                "description_saved": False,
                "embedding_updated": False
            }
        
        try:
            cursor = conn.cursor()
            
            # 1. 检查主题是否存在且未被删除
            cursor.execute("""
                SELECT theme_name, is_deleted 
                FROM futu_investment_themes 
                WHERE theme_id = %s AND market = %s
            """, (theme_id, market))
            
            theme_row = cursor.fetchone()
            if not theme_row:
                cursor.close()
                conn.close()
                return {
                    "success": False,
                    "message": "主题不存在",
                    "description_saved": False,
                    "embedding_updated": False
                }
            
            theme_name, is_deleted = theme_row
            if is_deleted:
                cursor.close()
                conn.close()
                return {
                    "success": False,
                    "message": "主题已被删除，无法编辑",
                    "description_saved": False,
                    "embedding_updated": False
                }
            
            # 2. 更新描述到数据库（两阶段提交：先保存描述）
            # 2.1 使用乐观锁检查版本（如果提供了version）
            if version is not None:
                cursor.execute("""
                    SELECT version FROM investment_theme_embeddings
                    WHERE theme_id = %s AND market = %s
                """, (theme_id, market))
                
                row = cursor.fetchone()
                if not row or row[0] != version:
                    cursor.close()
                    conn.close()
                    return {
                        "success": False,
                        "message": "版本冲突，主题可能已被其他用户修改，请刷新后重试",
                        "description_saved": False,
                        "embedding_updated": False
                    }
            
            # 2.2 更新描述到futu_investment_themes表
            cursor.execute("""
                UPDATE futu_investment_themes
                SET description = %s,
                    description_source = 'manual',
                    description_updated_at = NOW(),
                    updated_at = NOW()
                WHERE theme_id = %s AND market = %s
            """, (new_description, theme_id, market))
            
            # 2.3 更新版本和编辑追踪到investment_theme_embeddings表
            cursor.execute("""
                UPDATE investment_theme_embeddings
                SET last_manual_edit_at = NOW(),
                    last_manual_edit_by = %s,
                    version = version + 1,
                    updated_at = NOW()
                WHERE theme_id = %s AND market = %s
            """, (editor_username, theme_id, market))
            
            conn.commit()
            logger.info(f"描述保存成功: {theme_id}")
            
            # 3. 尝试重新生成向量
            embedding_success = False
            embedding_error = None
            
            try:
                # 直接使用用户提供的描述生成向量
                # 构建用于向量化的文本：包含主题名称、市场和描述
                embedding_text = f"{theme_name} ({market} market)\n\n{new_description}"
                
                # 调用Qwen Embedding API
                if 'qwen' not in llm_service.providers:
                    raise Exception("Qwen提供商未配置")
                
                embeddings = await llm_service.providers['qwen'].generate_embeddings(
                    texts=[embedding_text],
                    model=self.embedding_model,
                    text_type="document"
                )
                
                if not embeddings or len(embeddings) == 0:
                    raise Exception("向量生成失败，返回空结果")
                
                embedding = embeddings[0]
                
                # 验证向量维度
                if len(embedding) != self.embedding_dimension:
                    raise Exception(
                        f"向量维度不匹配: 期望{self.embedding_dimension}, 实际{len(embedding)}"
                    )
                
                # 4. 更新向量到数据库
                use_pgvector = self._detect_database_mode(cursor)
                
                if use_pgvector:
                    cursor.execute("""
                        UPDATE investment_theme_embeddings
                        SET embedding_vector = %s,
                            embedding_model = %s,
                            updated_at = NOW()
                        WHERE theme_id = %s AND market = %s
                    """, (embedding, self.embedding_model, theme_id, market))
                else:
                    import json
                    embedding_json = json.dumps(embedding)
                    cursor.execute("""
                        UPDATE investment_theme_embeddings
                        SET embedding_data = %s::jsonb,
                            embedding_model = %s,
                            updated_at = NOW()
                        WHERE theme_id = %s AND market = %s
                    """, (embedding_json, self.embedding_model, theme_id, market))
                
                conn.commit()
                embedding_success = True
                logger.info(f"向量更新成功: {theme_id}")
                
            except Exception as e:
                embedding_error = str(e)
                logger.error(f"向量生成/更新失败: {embedding_error}")
                # 不回滚，保留已保存的描述
            
            cursor.close()
            conn.close()
            
            # 5. 返回结果
            if embedding_success:
                return {
                    "success": True,
                    "message": "描述和向量更新成功",
                    "description_saved": True,
                    "embedding_updated": True
                }
            else:
                return {
                    "success": False,
                    "message": f"描述已保存，但向量更新失败: {embedding_error}",
                    "description_saved": True,
                    "embedding_updated": False,
                    "error": embedding_error
                }
            
        except Exception as e:
            logger.error(f"更新主题描述失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {
                "success": False,
                "message": f"更新失败: {str(e)}",
                "description_saved": False,
                "embedding_updated": False,
                "error": str(e)
            }
    
    def update_theme_enabled_status(
        self,
        theme_id: str,
        market: str,
        is_enabled: bool,
        operator_username: str
    ) -> Dict[str, Any]:
        """
        更新主题的启用/禁用状态
        
        Args:
            theme_id: 主题ID
            market: 市场代码
            is_enabled: 是否启用
            operator_username: 操作者用户名
            
        Returns:
            {"success": bool, "message": str}
        """
        logger.info(f"更新主题状态: {theme_id} ({market}) -> {is_enabled}")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return {"success": False, "message": "数据库连接失败"}
        
        try:
            cursor = conn.cursor()
            
            # 检查主题是否存在且未被删除
            cursor.execute("""
                SELECT theme_name, is_deleted 
                FROM futu_investment_themes 
                WHERE theme_id = %s AND market = %s
            """, (theme_id, market))
            
            row = cursor.fetchone()
            if not row:
                cursor.close()
                conn.close()
                return {"success": False, "message": "主题不存在"}
            
            if row[1]:  # is_deleted
                cursor.close()
                conn.close()
                return {"success": False, "message": "主题已被删除"}
            
            # 如果要启用，检查是否已向量化
            if is_enabled:
                cursor.execute("""
                    SELECT 1 FROM investment_theme_embeddings 
                    WHERE theme_id = %s AND market = %s
                """, (theme_id, market))
                
                if not cursor.fetchone():
                    cursor.close()
                    conn.close()
                    return {
                        "success": False,
                        "message": "主题尚未向量化，无法启用。请先同步主题数据。"
                    }
            
            # 更新状态
            cursor.execute("""
                UPDATE futu_investment_themes
                SET is_enabled_for_matching = %s,
                    updated_at = NOW()
                WHERE theme_id = %s AND market = %s
            """, (is_enabled, theme_id, market))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            status_text = "启用" if is_enabled else "禁用"
            logger.info(f"主题状态更新成功: {theme_id} -> {status_text}")
            
            return {
                "success": True,
                "message": f"主题已{status_text}"
            }
            
        except Exception as e:
            logger.error(f"更新主题状态失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {"success": False, "message": f"更新失败: {str(e)}"}
    
    def delete_theme(
        self,
        theme_id: str,
        market: str,
        operator_username: str
    ) -> Dict[str, Any]:
        """
        软删除投资主题及其关联数据
        
        删除操作包括：
        1. 软删除 futu_investment_themes（标记is_deleted=TRUE）
        2. 删除 investment_theme_embeddings 的向量数据
        3. 删除 news_theme_matches 的匹配记录
        
        Args:
            theme_id: 主题ID
            market: 市场代码
            operator_username: 操作者用户名
            
        Returns:
            {
                "success": bool,
                "message": str,
                "deleted_matches_count": int,
                "deleted_embeddings": bool
            }
        """
        logger.info(f"开始删除主题: {theme_id} ({market}) by {operator_username}")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return {"success": False, "message": "数据库连接失败"}
        
        try:
            cursor = conn.cursor()
            
            # 1. 首先检查主题是否在 investment_theme_embeddings 中存在（因为列表是从这里查询的）
            cursor.execute("""
                SELECT theme_name 
                FROM investment_theme_embeddings 
                WHERE theme_id = %s AND market = %s
            """, (theme_id, market))
            
            embedding_row = cursor.fetchone()
            if not embedding_row:
                cursor.close()
                conn.close()
                return {"success": False, "message": "主题不存在（未找到向量数据）"}
            
            theme_name = embedding_row[0]
            
            # 2. 检查主题在 futu_investment_themes 中的状态
            cursor.execute("""
                SELECT theme_name, is_deleted 
                FROM futu_investment_themes 
                WHERE theme_id = %s AND market = %s
            """, (theme_id, market))
            
            futu_row = cursor.fetchone()
            
            # 3. 如果主题在 futu_investment_themes 中存在且未被删除，则软删除它
            if futu_row and not futu_row[1]:  # 存在且未删除
                cursor.execute("""
                    UPDATE futu_investment_themes
                    SET is_deleted = TRUE,
                        deleted_at = NOW(),
                        deleted_by = %s,
                        is_enabled_for_matching = FALSE,
                        updated_at = NOW()
                    WHERE theme_id = %s AND market = %s
                """, (operator_username, theme_id, market))
            elif futu_row and futu_row[1]:  # 已软删除，跳过更新，直接删除向量数据
                logger.info(f"主题 {theme_id} 已在 futu_investment_themes 中被软删除，继续删除向量数据")
            
            # 4. 删除向量数据
            cursor.execute("""
                DELETE FROM investment_theme_embeddings
                WHERE theme_id = %s AND market = %s
            """, (theme_id, market))
            
            deleted_embeddings = cursor.rowcount > 0
            
            # 5. 删除新闻匹配记录
            cursor.execute("""
                SELECT COUNT(*) FROM news_theme_matches
                WHERE theme_id = %s
            """, (theme_id,))
            
            match_count = cursor.fetchone()[0]
            
            cursor.execute("""
                DELETE FROM news_theme_matches
                WHERE theme_id = %s
            """, (theme_id,))
            
            deleted_matches_count = cursor.rowcount
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(
                f"主题删除成功: {theme_id} - {theme_name}, "
                f"删除向量: {deleted_embeddings}, "
                f"删除匹配记录: {deleted_matches_count}条"
            )
            
            message_text = f"主题 '{theme_name}' 已删除"
            if futu_row and futu_row[1]:
                message_text += "（向量数据已清理）"
            
            return {
                "success": True,
                "message": message_text,
                "deleted_matches_count": deleted_matches_count,
                "deleted_embeddings": deleted_embeddings
            }
            
        except Exception as e:
            logger.error(f"删除主题失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {
                "success": False,
                "message": f"删除失败: {str(e)}"
            }
    
    def _enhance_theme_text(
        self, 
        theme_name: str, 
        market: str, 
        description: str
    ) -> str:
        """
        增强主题文本，用于向量化
        
        将主题名称、市场和描述组合成适合向量化的文本格式
        
        Args:
            theme_name: 主题名称
            market: 市场代码
            description: 主题描述
            
        Returns:
            增强后的文本
        """
        # 构建增强版文本：主题名称 + 市场信息 + 详细描述
        enhanced_text = f"{theme_name} ({market} market)\n\n{description}"
        
        logger.debug(f"增强文本生成: {theme_name}, 长度: {len(enhanced_text)}")
        
        return enhanced_text
    
    async def generate_theme_description(
        self,
        theme_name: str,
        market: str
    ) -> Dict[str, Any]:
        """
        使用LLM为新theme生成描述（预览，不保存）
        使用 theme_description_generator_v1 模板
        
        Args:
            theme_name: 主题名称
            market: 市场代码
            
        Returns:
            {
                "success": bool,
                "description": str,
                "message": str,
                "error": str (optional)
            }
        """
        logger.info(f"为新主题生成描述: {theme_name} ({market})")
        
        try:
            # 使用 theme_description_generator_v1 模板生成描述
            response = await llm_service.chat_completion_with_template(
                template_code=self.llm_template_code,
                variables={
                    'theme_name': theme_name,
                    'market': market,
                    'stock_symbols': 'Not available',  # 新主题还没有股票数据
                    'company_names': 'Not available'
                }
            )
            
            if not response or not response.content:
                raise Exception("LLM未返回描述内容")
            
            description = response.content.strip()
            
            # 验证生成的描述
            if len(description) < 50:
                raise Exception("生成的描述过短，请重试")
            
            logger.info(f"描述生成成功，长度: {len(description)}")
            
            return {
                "success": True,
                "description": description,
                "message": "描述生成成功"
            }
            
        except Exception as e:
            logger.error(f"生成主题描述失败: {str(e)}")
            return {
                "success": False,
                "description": "",
                "message": f"生成失败: {str(e)}",
                "error": str(e)
            }
    
    async def create_theme(
        self,
        theme_name: str,
        market: str,
        description: str,
        creator_username: str,
        theme_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        创建新的投资主题并生成向量
        
        Args:
            theme_name: 主题名称
            market: 市场代码
            description: 主题描述
            creator_username: 创建者用户名
            theme_id: 主题ID（可选，如果不提供则自动生成）
            
        Returns:
            {
                "success": bool,
                "message": str,
                "theme_id": str,
                "theme_saved": bool,
                "embedding_created": bool,
                "error": str (optional)
            }
        """
        logger.info(f"创建新主题: {theme_name} ({market})")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return {
                "success": False,
                "message": "数据库连接失败",
                "theme_saved": False,
                "embedding_created": False
            }
        
        try:
            cursor = conn.cursor()
            
            # 1. 生成theme_id（如果未提供）
            if not theme_id:
                # 生成格式：CUSTOM_<MARKET>_<TIMESTAMP>
                import time
                theme_id = f"CUSTOM_{market}_{int(time.time())}"
            
            # 2. 检查theme_id是否已存在
            cursor.execute("""
                SELECT 1 FROM futu_investment_themes 
                WHERE theme_id = %s AND market = %s
            """, (theme_id, market))
            
            if cursor.fetchone():
                cursor.close()
                conn.close()
                return {
                    "success": False,
                    "message": f"主题ID {theme_id} 已存在，请使用其他ID",
                    "theme_saved": False,
                    "embedding_created": False
                }
            
            # 3. 插入到futu_investment_themes表
            cursor.execute("""
                INSERT INTO futu_investment_themes 
                (theme_id, theme_name, market, stock_count, scraped_at, created_at, updated_at,
                 is_enabled_for_matching, is_deleted, description, description_source)
                VALUES (%s, %s, %s, 0, NOW(), NOW(), NOW(), TRUE, FALSE, %s, 'manual')
                RETURNING id
            """, (theme_id, theme_name, market, description))
            
            futu_theme_id = cursor.fetchone()[0]
            conn.commit()
            
            logger.info(f"主题保存成功: {theme_id} (DB ID: {futu_theme_id})")
            
            # 4. 尝试生成向量
            embedding_success = False
            embedding_error = None
            
            try:
                # 生成增强文本
                enhanced_text = self._enhance_theme_text(theme_name, market, description)
                
                # 调用Qwen Embedding API
                if 'qwen' not in llm_service.providers:
                    raise Exception("Qwen提供商未配置")
                
                embeddings = await llm_service.providers['qwen'].generate_embeddings(
                    texts=[enhanced_text],
                    model=self.embedding_model,
                    text_type="document"
                )
                
                if not embeddings or len(embeddings) == 0:
                    raise Exception("向量生成失败，返回空结果")
                
                embedding = embeddings[0]
                
                # 验证向量维度
                if len(embedding) != self.embedding_dimension:
                    raise Exception(
                        f"向量维度不匹配: 期望{self.embedding_dimension}, 实际{len(embedding)}"
                    )
                
                # 5. 保存向量到investment_theme_embeddings
                use_pgvector = self._detect_database_mode(cursor)
                
                if use_pgvector:
                    cursor.execute("""
                        INSERT INTO investment_theme_embeddings 
                        (theme_id, theme_name, market, embedding_vector, embedding_model,
                         last_manual_edit_at, last_manual_edit_by, version)
                        VALUES (%s, %s, %s, %s, %s, NOW(), %s, 1)
                    """, (theme_id, theme_name, market, embedding, 
                          self.embedding_model, creator_username))
                else:
                    import json
                    embedding_json = json.dumps(embedding)
                    cursor.execute("""
                        INSERT INTO investment_theme_embeddings 
                        (theme_id, theme_name, market, embedding_data, embedding_model,
                         last_manual_edit_at, last_manual_edit_by, version)
                        VALUES (%s, %s, %s, %s::jsonb, %s, NOW(), %s, 1)
                    """, (theme_id, theme_name, market, embedding_json,
                          self.embedding_model, creator_username))
                
                conn.commit()
                embedding_success = True
                logger.info(f"向量创建成功: {theme_id}")
                
            except Exception as e:
                embedding_error = str(e)
                logger.error(f"向量生成/保存失败: {embedding_error}")
                # 不回滚，保留已保存的主题
            
            cursor.close()
            conn.close()
            
            # 6. 返回结果
            if embedding_success:
                return {
                    "success": True,
                    "message": f"主题 '{theme_name}' 创建成功",
                    "theme_id": theme_id,
                    "theme_saved": True,
                    "embedding_created": True
                }
            else:
                return {
                    "success": False,
                    "message": f"主题已保存，但向量生成失败: {embedding_error}",
                    "theme_id": theme_id,
                    "theme_saved": True,
                    "embedding_created": False,
                    "error": embedding_error
                }
            
        except Exception as e:
            logger.error(f"创建主题失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {
                "success": False,
                "message": f"创建失败: {str(e)}",
                "theme_saved": False,
                "embedding_created": False,
                "error": str(e)
            }

