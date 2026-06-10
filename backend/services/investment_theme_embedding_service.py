# -*- coding: utf-8 -*-
"""
投资主题向量化服务
用于生成和管理投资主题的Embedding向量，支持基于向量检索的新闻主题匹配

注意：此版本不再依赖 futu_investment_themes 表，改为使用 investment_theme_embeddings 作为独立的主题管理表
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
        db_theme_id: int = None,
        category: Optional[str] = None,
        market: Optional[str] = None,  # 兼容旧调用
        force_regenerate: bool = False
    ) -> str:
        """
        生成增强版主题文本描述
        
        🌟 优化后的缓存策略（不再依赖 futu 表）：
        1. 优先使用 investment_theme_embeddings 表中已缓存的 description
        2. 如果缓存不存在或 force_regenerate=True，则调用 LLM 生成
        3. 生成成功后保存到数据库缓存
        4. LLM 失败时降级使用简单模板
        
        Args:
            cursor: 数据库游标
            theme_id: 主题ID（字符串，如 BK1232 或 CUSTOM_US_xxx）
            theme_name: 主题名称
            db_theme_id: 数据库主题ID（整数，可选）
            force_regenerate: 是否强制重新生成（忽略缓存）
            
        Returns:
            增强版主题文本描述
        """
        try:
            # 步骤1: 从 investment_theme_embeddings 获取缓存的描述
            cursor.execute("""
                SELECT id, description, description_source, category
                FROM investment_theme_embeddings
                WHERE theme_id = %s
                LIMIT 1
            """, (theme_id,))
            
            result = cursor.fetchone()
            if result:
                db_theme_id, cached_description, description_source, stored_category = result
            else:
                # 找不到主题记录，使用简化版本
                logger.warning(f"主题不存在: {theme_id}")
                return f"Investment theme: {theme_name}. Investment category focusing on {theme_name.lower()} sector."
            
            category_value = category or stored_category or "General investing"
            category_value = (category_value.strip() if isinstance(category_value, str) else str(category_value)).strip() or "General investing"
            
            # 步骤2: 如果有缓存的描述且不强制重新生成，直接返回
            if cached_description and not force_regenerate:
                logger.debug(f"✅ 使用缓存描述: {theme_name} (来源: {description_source or 'unknown'})")
                return cached_description
            
            # 步骤3: 生成新描述（不再依赖股票信息，因为 futu_theme_stocks 表已删除）
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
                            'category': category_value,
                            'stock_symbols': 'Not available',
                            'company_names': 'Not available'
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
                category_lower = category_value.lower()
                generated_description = (
                    f"Investment theme: {theme_name}. "
                    f"Investment category focusing on {category_lower} sector."
                )
                logger.debug(f"使用简单模板: {theme_name}")
                description_source_new = 'simple_template'
            
            # 步骤6: 保存生成的描述到数据库缓存
            if generated_description and db_theme_id:
                try:
                    cursor.execute("""
                        UPDATE investment_theme_embeddings
                        SET 
                            description = %s,
                            description_source = %s,
                            updated_at = NOW()
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
            return f"Investment theme: {theme_name}"
    
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
        从 investment_theme_embeddings 表读取主题，生成并存储向量
        
        注意：此方法已更新，不再依赖 futu_investment_themes 表
        
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
            
            # 1. 获取所有主题（从 investment_theme_embeddings 表）
            cursor.execute("""
                SELECT DISTINCT id, theme_id, theme_name, category
                FROM investment_theme_embeddings
                ORDER BY theme_name
            """)
            
            themes = cursor.fetchall()
            total_themes = len(themes)
            
            if total_themes == 0:
                logger.warning("未找到任何投资主题")
                return {"success": True, "message": "没有需要处理的主题", "total": 0}
            
            logger.info(f"找到 {total_themes} 个投资主题")
            
            # 2. 检测数据库模式（确定使用 pgvector 还是 JSONB）
            use_pgvector = self._detect_database_mode(cursor)
            vector_column = 'embedding_vector' if use_pgvector else 'embedding_data'
            
            # 3. 检查哪些主题需要生成向量
            themes_to_process = []
            
            if force_update:
                themes_to_process = themes
                logger.info(f"强制更新模式，将处理所有 {total_themes} 个主题")
            else:
                # 只处理向量为空的主题（embedding_* 列为 NULL）
                for theme in themes:
                    db_id = theme[0]
                    
                    # 检查是否已有向量数据
                    cursor.execute(f"""
                        SELECT 
                            CASE 
                                WHEN {vector_column} IS NOT NULL THEN TRUE
                                ELSE FALSE
                            END as has_embedding
                        FROM investment_theme_embeddings
                        WHERE id = %s
                    """, (db_id,))
                    
                    result = cursor.fetchone()
                    if not result or not result[0]:  # 没有向量数据
                        themes_to_process.append(theme)
                
                logger.info(f"发现 {len(themes_to_process)} 个主题需要生成向量")
            
            if len(themes_to_process) == 0:
                logger.info("所有主题向量已存在，无需更新")
                return {
                    "success": True,
                    "message": "所有主题向量已存在",
                    "total": total_themes,
                    "processed": 0,
                    "skipped": total_themes
                }
            
            # 4. 批量生成向量（每次10个，Qwen API限制）
            batch_size = 10  # Qwen API要求批次大小不超过10
            processed_count = 0
            failed_count = 0
            
            for i in range(0, len(themes_to_process), batch_size):
                batch = themes_to_process[i:i+batch_size]
                # 现在每个 theme 是 (id, theme_id, theme_name, category)
                batch_db_ids = [theme[0] for theme in batch]
                batch_theme_ids = [theme[1] for theme in batch]
                batch_theme_names = [theme[2] for theme in batch]
                batch_categories = [theme[3] for theme in batch]
                
                # 🔥 优化：构建增强版文本描述（包含股票信息）
                # 🌟 支持缓存：如果 force_update=True，强制重新生成描述
                # 🚀 使用并发处理加速描述生成
                description_tasks = []
                for db_id, theme_id, theme_name, category in zip(batch_db_ids, batch_theme_ids, batch_theme_names, batch_categories):
                    # 使用新的增强文本生成方法，传入数据库ID
                    # force_regenerate=force_update 确保强制更新时重新生成描述
                    task = self._generate_enriched_theme_text(
                        cursor=cursor,
                        theme_id=theme_id,
                        theme_name=theme_name,
                        db_theme_id=db_id,
                        category=category,
                        force_regenerate=force_update
                    )
                    description_tasks.append(task)
                
                # 并发执行所有描述生成任务
                batch_descriptions = await asyncio.gather(*description_tasks, return_exceptions=True)
                
                # 处理异常，将异常结果替换为简单描述，并组合 theme_name + description
                batch_texts = []
                for idx, description in enumerate(batch_descriptions):
                    theme_name = batch_theme_names[idx]
                    if isinstance(description, Exception):
                        logger.warning(f"描述生成失败，使用简单描述: {theme_name} - {str(description)}")
                        # 即使描述生成失败，也要包含 theme_name
                        batch_texts.append(f"{theme_name}")
                    else:
                        # 🔥 修复：使用 theme_name + description 组合，与 create_theme 保持一致
                        enhanced_text = self._enhance_theme_text(theme_name, description)
                        batch_texts.append(enhanced_text)
                
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
                                (theme_id, theme_name, embedding_vector, embedding_model)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (theme_id) 
                                DO UPDATE SET 
                                    embedding_vector = EXCLUDED.embedding_vector,
                                    embedding_model = EXCLUDED.embedding_model,
                                    updated_at = NOW()
                            """, (theme_id, theme_name, embedding, self.embedding_model))
                        else:
                            # JSONB模式：使用 embedding_data 列
                            import json
                            embedding_json = json.dumps(embedding)
                            cursor.execute("""
                                INSERT INTO investment_theme_embeddings 
                                (theme_id, theme_name, embedding_data, embedding_model)
                                VALUES (%s, %s, %s::jsonb, %s)
                                ON CONFLICT (theme_id) 
                                DO UPDATE SET 
                                    embedding_data = EXCLUDED.embedding_data,
                                    embedding_model = EXCLUDED.embedding_model,
                                    updated_at = NOW()
                            """, (theme_id, theme_name, embedding_json, self.embedding_model))
                        
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
            market: 市场过滤（已废弃，market 列已删除，此参数不再生效）
            top_k: 返回Top-K个主题
            min_similarity: 最小相似度阈值（0-1）
            save_to_db: 是否保存匹配结果到数据库
            
        Returns:
            匹配的主题列表，按相似度降序排列
            
        Example:
            matches = await service.match_news_to_themes(
                news_text="Tesla发布新款电动车",
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
            
            # 🔥 修复：统一使用 "document" 模式，确保与主题向量在同一向量空间
            # 注意：虽然这是查询文本，但为了与文档向量保持一致，使用 "document" 模式
            embeddings = await llm_service.providers['qwen'].generate_embeddings(
                texts=[news_text],
                model=self.embedding_model,
                text_type="document"  # 统一使用 document 模式，确保向量空间一致
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
            
            # 🔍 先查询候选主题总数（用于调试）
            # 注意：market 列已删除，不再进行市场过滤
            cursor.execute("""
                SELECT COUNT(*) 
                FROM investment_theme_embeddings e
                WHERE e.is_enabled_for_matching = TRUE
            """)
            total_candidates = cursor.fetchone()[0]
            
            # 根据数据库模式选择不同的查询方式
            if use_pgvector:
                # pgvector模式：使用向量操作符
                # 直接从 investment_theme_embeddings 过滤启用且未删除的主题
                # 注意：market 列已删除，不再进行市场过滤
                # 使用pgvector的余弦相似度操作符 <=>
                # 1 - (v1 <=> v2) = 余弦相似度
                # 过滤条件：is_enabled_for_matching = TRUE
                query = """
                    SELECT 
                        e.theme_id,
                        e.theme_name,
                        e.category,
                        1 - (e.embedding_vector <=> %s::vector) as similarity
                    FROM investment_theme_embeddings e
                    WHERE e.is_enabled_for_matching = TRUE
                    ORDER BY e.embedding_vector <=> %s::vector
                    LIMIT %s
                """
                
                cursor.execute(query, [news_vector, news_vector, top_k])
            else:
                # JSONB模式：先获取所有主题，然后在Python中计算相似度
                # 过滤条件：is_enabled_for_matching = TRUE
                # 注意：market 列已删除，不再进行市场过滤
                import json
                import numpy as np
                
                query = """
                    SELECT 
                        e.theme_id,
                        e.theme_name,
                        e.category,
                        e.embedding_data
                    FROM investment_theme_embeddings e
                    WHERE e.is_enabled_for_matching = TRUE
                """
                
                cursor.execute(query)
                
                # 在Python中计算余弦相似度
                theme_similarities = []
                for row in cursor.fetchall():
                    theme_id = row[0]
                    theme_name = row[1]
                    category = row[2]
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
                        'category': category,
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
                    category = row[2]
                    similarity = float(row[3])
                    
                    # 过滤低于阈值的结果
                    if similarity < min_similarity:
                        continue
                    
                    match_result = {
                        'theme_id': theme_id,
                        'theme_name': theme_name,
                        'category': category,
                        'market': None,  # market 列已删除
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
                        'category': theme['category'],
                        'market': None,  # market 列已删除
                        'similarity': round(similarity, 4),
                        'confidence': round(similarity, 2),
                        'rank': len(results) + 1  # 使用results长度+1作为排名
                    }
                    
                    results.append(match_result)
                    
                    # 限制结果数量为top_k
                    if len(results) >= top_k:
                        break
            
            # 🔍 调试信息：记录匹配统计
            if not use_pgvector:
                total_candidates = len(theme_similarities)
            
            logger.info(
                f"匹配统计: 找到 {len(results)} 个匹配主题（阈值: {min_similarity}），"
                f"候选主题总数: {total_candidates}，"
                f"查询文本长度: {len(news_text)}"
            )
            
            # 如果匹配结果为空，记录更多调试信息
            if len(results) == 0:
                logger.warning(
                    f"⚠️ 未找到匹配主题！可能原因："
                    f"1. 相似度阈值过高（当前: {min_similarity}）"
                    f"2. 主题向量未正确生成（可能只用了description，没有theme_name）"
                    f"3. 查询文本与主题描述差异过大"
                    f"建议：检查主题向量是否使用 theme_name + description 生成"
                )
            
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
        theme_name: str
    ) -> bool:
        """
        更新单个主题的向量
        
        Args:
            theme_id: 主题ID
            theme_name: 主题名称
            
        Returns:
            是否成功
        """
        try:
            # 从数据库获取 description
            conn = self.db_manager.get_connection()
            if not conn:
                return False
            
            cursor = conn.cursor()
            
            # 获取主题描述
            cursor.execute("""
                SELECT description
                FROM investment_theme_embeddings
                WHERE theme_id = %s
                LIMIT 1
            """, (theme_id,))
            
            result = cursor.fetchone()
            description = result[0] if result and result[0] else None
            
            # 🔥 修复：使用 theme_name + description 组合，与 create_theme 保持一致
            if description:
                enhanced_text = self._enhance_theme_text(theme_name, description)
            else:
                # 如果没有描述，只使用 theme_name
                enhanced_text = f"{theme_name}"
                logger.warning(f"主题 {theme_id} 没有描述，仅使用名称向量化")
            
            cursor.close()
            conn.close()
            
            # 生成向量
            if 'qwen' not in llm_service.providers:
                raise Exception("Qwen提供商未配置")
            
            embeddings = await llm_service.providers['qwen'].generate_embeddings(
                texts=[enhanced_text],
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
                    (theme_id, theme_name, embedding_vector, embedding_model)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (theme_id) 
                    DO UPDATE SET 
                        theme_name = EXCLUDED.theme_name,
                        embedding_vector = EXCLUDED.embedding_vector,
                        embedding_model = EXCLUDED.embedding_model,
                        updated_at = NOW()
                """, (theme_id, theme_name, embedding, self.embedding_model))
            else:
                # JSONB模式
                import json
                embedding_json = json.dumps(embedding)
                cursor.execute("""
                    INSERT INTO investment_theme_embeddings 
                    (theme_id, theme_name, embedding_data, embedding_model)
                    VALUES (%s, %s, %s::jsonb, %s)
                    ON CONFLICT (theme_id) 
                    DO UPDATE SET 
                        theme_name = EXCLUDED.theme_name,
                        embedding_data = EXCLUDED.embedding_data,
                        embedding_model = EXCLUDED.embedding_model,
                        updated_at = NOW()
                """, (theme_id, theme_name, embedding_json, self.embedding_model))
            
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
        new_description: str,
        editor_username: str,
        new_group: Optional[str] = None,
        version: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        更新主题描述并重新生成向量
        
        Args:
            theme_id: 主题ID
            new_description: 新的描述内容
            editor_username: 编辑者用户名
            new_group: 新的分组（可选）
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
        logger.info(f"开始更新主题描述: {theme_id}")
        
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
            
            # 1. 检查主题是否存在
            cursor.execute("""
                SELECT theme_name, version
                FROM investment_theme_embeddings
                WHERE theme_id = %s
            """, (theme_id,))
            
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
            
            theme_name, current_version = theme_row
            
            # 2. 更新描述到数据库（使用乐观锁）
            # 2.1 检查版本冲突（如果提供了version）
            if version is not None and current_version != version:
                cursor.close()
                conn.close()
                return {
                    "success": False,
                    "message": "版本冲突，主题可能已被其他用户修改，请刷新后重试",
                    "description_saved": False,
                    "embedding_updated": False
                }
            
            # 2.2 更新描述和元数据到 investment_theme_embeddings 表
            # new_group 的处理逻辑：
            # - 如果是空字符串（用户清空了 group）：设置为 NULL
            # - 如果是非空字符串：使用该值
            # - 如果是 None（不应该出现，因为前端总是发送值）：不更新
            # 
            # 注意：编辑弹窗中总是显示当前的 group 值，所以前端会始终发送 group 参数
            if new_group is not None:
                # 处理 group 值：空字符串转为 None，非空字符串使用原值
                group_value = new_group.strip() if new_group.strip() else None
                cursor.execute("""
                    UPDATE investment_theme_embeddings
                    SET description = %s,
                        "group" = %s,
                        description_source = 'manual',
                        last_manual_edit_at = NOW(),
                        last_manual_edit_by = %s,
                        version = version + 1,
                        updated_at = NOW()
                    WHERE theme_id = %s
                """, (new_description, group_value, editor_username, theme_id))
            else:
                # 如果未提供 new_group，只更新描述（理论上不应该走到这里）
                cursor.execute("""
                    UPDATE investment_theme_embeddings
                    SET description = %s,
                        description_source = 'manual',
                        last_manual_edit_at = NOW(),
                        last_manual_edit_by = %s,
                        version = version + 1,
                        updated_at = NOW()
                    WHERE theme_id = %s
                """, (new_description, editor_username, theme_id))
            
            conn.commit()
            logger.info(f"描述保存成功: {theme_id}")
            
            # 3. 尝试重新生成向量
            embedding_success = False
            embedding_error = None
            
            try:
                # 直接使用用户提供的描述生成向量
                # 构建用于向量化的文本：包含主题名称和描述
                embedding_text = f"{theme_name}\n\n{new_description}"
                
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
                        WHERE theme_id = %s
                    """, (embedding, self.embedding_model, theme_id))
                else:
                    import json
                    embedding_json = json.dumps(embedding)
                    cursor.execute("""
                        UPDATE investment_theme_embeddings
                        SET embedding_data = %s::jsonb,
                            embedding_model = %s,
                            updated_at = NOW()
                        WHERE theme_id = %s
                    """, (embedding_json, self.embedding_model, theme_id))
                
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
        is_enabled: bool,
        operator_username: str
    ) -> Dict[str, Any]:
        """
        更新主题的启用/禁用状态
        
        Args:
            theme_id: 主题ID
            is_enabled: 是否启用
            operator_username: 操作者用户名
            
        Returns:
            {"success": bool, "message": str}
        """
        logger.info(f"更新主题状态: {theme_id} -> {is_enabled}")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return {"success": False, "message": "数据库连接失败"}
        
        try:
            cursor = conn.cursor()
            
            # 检测数据库模式以确定列名
            use_pgvector = self._detect_database_mode(cursor)
            
            if use_pgvector:
                cursor.execute("""
                    SELECT theme_name, 
                           CASE 
                               WHEN embedding_vector IS NOT NULL THEN TRUE
                               ELSE FALSE
                           END as has_embedding
                    FROM investment_theme_embeddings
                    WHERE theme_id = %s
                """, (theme_id,))
            else:
                cursor.execute("""
                    SELECT theme_name, 
                           CASE 
                               WHEN embedding_data IS NOT NULL THEN TRUE
                               ELSE FALSE
                           END as has_embedding
                    FROM investment_theme_embeddings
                    WHERE theme_id = %s
                """, (theme_id,))
            
            row = cursor.fetchone()
            if not row:
                cursor.close()
                conn.close()
                return {"success": False, "message": "主题不存在"}
            
            theme_name, has_embedding = row
            
            # 如果要启用，检查是否已向量化
            if is_enabled and not has_embedding:
                cursor.close()
                conn.close()
                return {
                    "success": False,
                    "message": "主题尚未向量化，无法启用。请先生成向量。"
                }
            
            # 更新状态
            cursor.execute("""
                UPDATE investment_theme_embeddings
                SET is_enabled_for_matching = %s,
                    updated_at = NOW()
                WHERE theme_id = %s
            """, (is_enabled, theme_id))
            
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
        operator_username: str
    ) -> Dict[str, Any]:
        """
        硬删除投资主题及其关联数据（物理删除，不可恢复）。
        
        Args:
            theme_id: 主题ID
            operator_username: 操作者用户名
            
        Returns:
            {"success": bool, "message": str, "deleted_embeddings": bool}
        """
        logger.info(f"开始硬删除主题: {theme_id} by {operator_username}")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return {"success": False, "message": "数据库连接失败"}
        
        try:
            cursor = conn.cursor()
            
            # 1. 检查主题是否存在并获取名称
            cursor.execute("""
                SELECT theme_name
                FROM investment_theme_embeddings 
                WHERE theme_id = %s
            """, (theme_id,))
            
            row = cursor.fetchone()
            if not row:
                cursor.close()
                conn.close()
                return {"success": False, "message": "主题不存在"}
            
            theme_name = row[0]
            
            # 硬删除主题（物理删除）
            # 先级联删除 content_theme_matches
            cursor.execute(
                "DELETE FROM content_theme_matches WHERE theme_id = %s",
                (theme_id,),
            )
            # 再删除主题本身
            cursor.execute("""
                DELETE FROM investment_theme_embeddings
                WHERE theme_id = %s
            """, (theme_id,))
            
            deleted_embeddings = cursor.rowcount > 0
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"主题硬删除成功: {theme_id} - {theme_name}, 删除记录: {deleted_embeddings}")
            
            return {
                "success": True,
                "message": f"主题 '{theme_name}' 已永久删除",
                "deleted_embeddings": deleted_embeddings
            }
            
        except Exception as e:
            logger.error(f"硬删除主题失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {
                "success": False,
                "message": f"删除失败: {str(e)}"
            }
    
    def batch_delete_themes(
        self,
        theme_keys: List[str],
        operator_username: str
    ) -> Dict[str, Any]:
        """
        批量硬删除投资主题及其关联数据（物理删除，不可恢复）
        
        Args:
            theme_keys: 主题ID列表 [theme_id, ...]
            operator_username: 操作者用户名
            
        Returns:
            {
                "success": bool,
                "message": str,
                "deleted_count": int,
                "failed_count": int,
                "details": List[dict]
            }
        """
        logger.info(f"开始批量硬删除 {len(theme_keys)} 个主题 by {operator_username}")
        
        conn = self.db_manager.get_connection()
        if not conn:
            return {
                "success": False,
                "message": "数据库连接失败",
                "deleted_count": 0,
                "failed_count": len(theme_keys),
            }
        
        try:
            cursor = conn.cursor()
            
            deleted_count = 0
            failed_count = 0
            details = []
            
            for theme_id in theme_keys:
                try:
                    # 1. 检查主题是否存在并获取名称
                    cursor.execute("""
                        SELECT theme_name
                        FROM investment_theme_embeddings 
                        WHERE theme_id = %s
                    """, (theme_id,))
                    
                    row = cursor.fetchone()
                    if not row:
                        details.append({
                            "theme_id": theme_id,
                            "success": False,
                            "message": "主题不存在"
                        })
                        failed_count += 1
                        continue
                    
                    theme_name = row[0]

                    # 硬删除主题（物理删除）
                    # 先级联删除 content_theme_matches
                    cursor.execute(
                        "DELETE FROM content_theme_matches WHERE theme_id = %s",
                        (theme_id,),
                    )
                    # 再删除主题本身
                    cursor.execute("""
                        DELETE FROM investment_theme_embeddings
                        WHERE theme_id = %s
                    """, (theme_id,))
                    
                    details.append({
                        "theme_id": theme_id,
                        "theme_name": theme_name,
                        "success": True,
                        "message": "永久删除成功"
                    })
                    deleted_count += 1
                    
                    logger.info(f"✅ 硬删除成功: {theme_id} - {theme_name}")
                    
                except Exception as e:
                    details.append({
                        "theme_id": theme_id,
                        "success": False,
                        "message": f"删除失败: {str(e)}"
                    })
                    failed_count += 1
                    logger.error(f"❌ 硬删除失败: {theme_id} - {str(e)}")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            success = failed_count == 0
            
            if success:
                message = f"成功批量永久删除 {deleted_count} 个主题"
            else:
                message = f"批量删除完成: 成功 {deleted_count} 个，失败 {failed_count} 个"
            
            logger.info(f"批量硬删除完成: 成功 {deleted_count}/{len(theme_keys)} 个主题")
            
            return {
                "success": success,
                "message": message,
                "deleted_count": deleted_count,
                "failed_count": failed_count,
                "details": details
            }
            
        except Exception as e:
            logger.error(f"批量硬删除主题失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {
                "success": False,
                "message": f"批量删除失败: {str(e)}",
                "deleted_count": 0,
                "failed_count": len(theme_keys),
                "details": []
            }
    
    def _enhance_theme_text(
        self, 
        theme_name: str, 
        description: str
    ) -> str:
        """
        增强主题文本，用于向量化
        
        将主题名称和描述组合成适合向量化的文本格式
        
        Args:
            theme_name: 主题名称
            description: 主题描述
            
        Returns:
            增强后的文本
        """
        # 构建增强版文本：主题名称 + 详细描述
        enhanced_text = f"{theme_name}\n\n{description}"
        
        logger.debug(f"增强文本生成: {theme_name}, 长度: {len(enhanced_text)}")
        
        return enhanced_text
    
    async def generate_theme_description(
        self,
        theme_name: str,
        category: str,
        stock_symbols: Optional[str] = None,
        company_names: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        使用LLM为新theme生成描述（预览，不保存）
        使用 theme_description_generator_v1 模板
        
        Args:
            theme_name: 主题名称
            category: 主题分类/类别
            stock_symbols: 相关股票代码（逗号分隔，如 "NVDA.US, AMD.US"）
            company_names: 相关公司名称（逗号分隔，如 "NVIDIA, AMD"）
            
        Returns:
            {
                "success": bool,
                "description": str,
                "message": str,
                "error": str (optional)
            }
        """
        logger.info(f"为新主题生成描述: {theme_name}, 分类: {category}, 股票: {stock_symbols or 'N/A'}")
        
        try:
            # 准备变量
            variables = {
                'theme_name': theme_name,
                'category': category,
                'stock_symbols': stock_symbols or 'Not available',
                'company_names': company_names or 'Not available'
            }
            
            # 使用 theme_description_generator_v1 模板生成描述
            response = await llm_service.chat_completion_with_template(
                template_code=self.llm_template_code,
                variables=variables
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
        category: str,
        description: str,
        creator_username: str,
        group: Optional[str] = None,
        theme_id: Optional[str] = None,
        market: Optional[str] = None,
        tickers: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        创建新的投资主题并生成向量
        
        Args:
            theme_name: 主题名称
            category: 主题分类/类别
            description: 主题描述
            creator_username: 创建者用户名
            group: 主题分组（如 AI、crypto 等，可选，不参与向量化）
            theme_id: 主题ID（可选，如果不提供则自动生成）
            market: 市场代码（US/HK/CN，可选）
            tickers: 关联的股票代码列表（简写，可选）
            
        Returns:
            {
                "success": bool,
                "message": str,
                "theme_id": str,
                "theme_saved": bool,
                "embedding_created": bool,
                "associations_created": int,
                "error": str (optional)
            }
        """
        logger.info(f"创建新主题: 分组={group or 'N/A'}, {theme_name}, 分类: {category}, 市场: {market or 'N/A'}, 股票数: {len(tickers) if tickers else 0}")
        
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
                # 生成格式：username_theme_name_category
                # 与批量上传使用相同的逻辑，确保一致性
                # 例如：'john_ai_healthcare_diagnostics'
                username_part = creator_username.replace(' ', '_').lower() if creator_username else 'unknown'
                theme_name_part = theme_name.replace(' ', '_').lower()
                category_part = category.replace(' ', '_').lower()
                theme_id = f"{username_part}_{theme_name_part}_{category_part}"
            
            # 2. 检查theme_id是否已存在
            cursor.execute("""
                SELECT id, theme_name, category, description, "group", created_by
                FROM investment_theme_embeddings
                WHERE theme_id = %s
            """, (theme_id,))
            
            existing_theme = cursor.fetchone()
            if existing_theme:
                # 获取现有主题的 tickers
                db_theme_id = existing_theme[0]
                existing_theme_name = existing_theme[1]
                existing_category = existing_theme[2]
                existing_description = existing_theme[3]
                existing_group = existing_theme[4]
                existing_created_by = existing_theme[5]
                
                cursor.execute("""
                    SELECT ticker FROM theme_company_associations
                    WHERE theme_id = %s
                    ORDER BY ticker
                """, (theme_id,))
                
                existing_tickers = [row[0] for row in cursor.fetchall()]
                
                cursor.close()
                conn.close()
                
                # 返回冲突信息，让前端处理
                return {
                    "success": False,
                    "conflict": True,
                    "message": f"主题 '{theme_name}' (Category: {category}) 已存在",
                    "theme_id": theme_id,
                    "theme_saved": False,
                    "embedding_created": False,
                    "existing_theme": {
                        "db_theme_id": db_theme_id,
                        "theme_id": theme_id,
                        "theme_name": existing_theme_name,
                        "category": existing_category,
                        "description": existing_description,
                        "group": existing_group,
                        "created_by": existing_created_by,
                        "tickers": existing_tickers
                    },
                    "new_tickers": tickers or []
                }
            
            # 3. 生成向量
            embedding_success = False
            embedding_error = None
            embedding = None
            
            try:
                # 生成增强文本
                enhanced_text = self._enhance_theme_text(theme_name, description)
                
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
                
                embedding_success = True
                logger.info(f"向量生成成功: {theme_id}")
                
            except Exception as e:
                embedding_error = str(e)
                logger.error(f"向量生成失败: {embedding_error}")
                # 继续执行，即使向量生成失败也要保存主题
            
            # 4. 保存主题和向量到 investment_theme_embeddings 表
            use_pgvector = self._detect_database_mode(cursor)
            
            try:
                if use_pgvector and embedding:
                    # 有向量且使用 pgvector
                    cursor.execute("""
                        INSERT INTO investment_theme_embeddings 
                        (theme_id, theme_name, category, description, description_source,
                         embedding_vector, embedding_model, is_enabled_for_matching,
                         created_by, last_manual_edit_at, last_manual_edit_by, version, "group")
                        VALUES (%s, %s, %s, %s, 'manual', %s, %s, TRUE, %s, NOW(), %s, 1, %s)
                    """, (theme_id, theme_name, category, description, embedding, 
                          self.embedding_model, creator_username, creator_username, group))
                elif not use_pgvector and embedding:
                    # 有向量且使用 JSONB
                    import json
                    embedding_json = json.dumps(embedding)
                    cursor.execute("""
                        INSERT INTO investment_theme_embeddings 
                        (theme_id, theme_name, category, description, description_source,
                         embedding_data, embedding_model, is_enabled_for_matching,
                         created_by, last_manual_edit_at, last_manual_edit_by, version, "group")
                        VALUES (%s, %s, %s, %s, 'manual', %s::jsonb, %s, TRUE, %s, NOW(), %s, 1, %s)
                    """, (theme_id, theme_name, category, description, embedding_json,
                          self.embedding_model, creator_username, creator_username, group))
                else:
                    # 没有向量，只保存主题基本信息
                    cursor.execute("""
                        INSERT INTO investment_theme_embeddings 
                        (theme_id, theme_name, category, description, description_source,
                         is_enabled_for_matching,
                         created_by, last_manual_edit_at, last_manual_edit_by, version, "group")
                        VALUES (%s, %s, %s, %s, 'manual', FALSE, %s, NOW(), %s, 1, %s)
                    """, (theme_id, theme_name, category, description,
                          creator_username, creator_username, group))
                
                conn.commit()
                embedding_success = embedding is not None
                logger.info(f"主题保存成功: {theme_id}")
                
            except Exception as e:
                embedding_error = str(e)
                logger.error(f"向量生成/保存失败: {embedding_error}")
                # 不回滚，保留已保存的主题
            
            cursor.close()
            conn.close()
            
            # 6. 创建主题-公司关联（如果提供了 tickers）
            associations_created = 0
            if tickers:
                try:
                    from backend.services.theme_company_association_service import ThemeCompanyAssociationService
                    from backend.utils.database_config import get_database_config
                    
                    db_config = get_database_config()
                    association_service = ThemeCompanyAssociationService(db_config)
                    
                    # 将 tickers 按市场分组
                    # 支持带后缀的 ticker (如 NVDA.US, 0700.HK) 和不带后缀的 ticker (如 NVDA)
                    tickers_by_market = {}
                    
                    for ticker in tickers:
                        ticker = ticker.strip()
                        if not ticker:
                            continue
                        
                        # 检查 ticker 是否包含市场后缀
                        if '.' in ticker:
                            # 带后缀：从后缀提取 market
                            parts = ticker.rsplit('.', 1)
                            if len(parts) == 2:
                                ticker_code = parts[0]
                                ticker_suffix = parts[1].upper()
                                
                                # 映射交易所代码到市场代码
                                # 支持简化代码和EODHD完整代码
                                exchange_to_market = {
                                    'US': 'US', 'NASDAQ': 'US', 'NYSE': 'US', 'AMEX': 'US',
                                    'HK': 'HK', 'HKEX': 'HK',
                                    'CN': 'CN', 'SH': 'CN', 'SZ': 'CN',
                                    'SHG': 'CN', 'SHE': 'CN',  # EODHD交易所代码
                                }
                                
                                ticker_market = exchange_to_market.get(ticker_suffix)
                                
                                if ticker_market:
                                    if ticker_market not in tickers_by_market:
                                        tickers_by_market[ticker_market] = []
                                    # 保存完整的 ticker（包含原始后缀）
                                    tickers_by_market[ticker_market].append(ticker)
                                else:
                                    logger.warning(f"未识别的市场后缀: {ticker_suffix}，跳过 ticker: {ticker}")
                            else:
                                logger.warning(f"无效的 ticker 格式: {ticker}")
                        else:
                            # 不带后缀：使用传入的 market 参数（如果有）
                            if market:
                                if market not in tickers_by_market:
                                    tickers_by_market[market] = []
                                tickers_by_market[market].append(ticker)
                            else:
                                logger.warning(f"Ticker {ticker} 没有市场后缀且未指定 market 参数，跳过")
                    
                    # 为每个市场创建关联
                    for ticker_market, market_tickers in tickers_by_market.items():
                        association_result = association_service.create_associations(
                            theme_id=theme_id,
                            tickers=market_tickers,
                            market=ticker_market,
                            created_by=creator_username,
                            allow_unverified=True  # 允许创建未验证的关联
                        )
                        
                        if association_result.get("success"):
                            created_count = association_result.get("created_count", 0)
                            associations_created += created_count
                            logger.info(f"创建 {ticker_market} 市场关联成功: {created_count} 个")
                        else:
                            logger.warning(f"创建 {ticker_market} 市场关联失败: {association_result.get('error', 'Unknown error')}")
                    
                    logger.info(f"总共创建关联: {associations_created} 个")
                        
                except Exception as assoc_error:
                    logger.error(f"创建关联时出错: {str(assoc_error)}")
                    # 不影响主题创建，继续返回
            
            # 7. 返回结果
            if embedding_success:
                return {
                    "success": True,
                    "message": f"主题 '{theme_name}' 创建成功",
                    "theme_id": theme_id,
                    "theme_saved": True,
                    "embedding_created": True,
                    "associations_created": associations_created
                }
            else:
                return {
                    "success": False,
                    "message": f"主题已保存，但向量生成失败: {embedding_error}",
                    "theme_id": theme_id,
                    "theme_saved": True,
                    "embedding_created": False,
                    "associations_created": associations_created,
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

