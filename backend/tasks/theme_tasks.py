# -*- coding: utf-8 -*-
"""
Investment Themes Vector Retrieval Celery Tasks
用于大批量主题向量初始化、描述生成等耗时操作
"""

import asyncio
from celery import current_task
from backend.celery_app import app
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("theme_tasks")


def update_task_progress(task_id, status=None, progress=None, message=None, **kwargs):
    """
    Update task progress to Redis
    
    Args:
        task_id: Task ID
        status: Task status (initializing/processing/completed/failed)
        progress: Progress percentage (0-100)
        message: Log message
        **kwargs: Additional metadata
    """
    try:
        meta = {
            'status': status or 'processing',
            'progress': progress or 0,
            'message': message or '',
            **kwargs
        }
        
        current_task.update_state(
            state='PROGRESS',
            meta=meta
        )
        
        if message:
            logger.info(f"Task {task_id}: {message}")
            
    except Exception as e:
        logger.error(f"Failed to update task status: {str(e)}")


@app.task(bind=True, name='initialize_theme_embeddings', queue='ai_processing')
def initialize_theme_embeddings_task(self, force_update: bool = False):
    """
    Celery task: Initialize all investment theme embeddings
    
    适用场景：
    - 首次初始化所有主题向量
    - 更新已存在的主题向量
    - 大批量主题处理（数百到数千个）
    
    Args:
        force_update: Whether to force update existing embeddings
        
    Returns:
        dict: Processing statistics
            - success: bool
            - total: int - Total themes
            - processed: int - Successfully processed
            - failed: int - Failed
            - skipped: int - Skipped (already exists)
    """
    task_id = self.request.id
    
    try:
        logger.info(f"Starting theme embeddings initialization task {task_id}, force_update={force_update}")
        
        # Import service
        from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
        
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        # Update initial status
        update_task_progress(
            task_id,
            status="initializing",
            progress=0,
            message=f"Initializing theme embeddings initialization task..."
        )
        
        # Execute async initialization
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                service.initialize_all_theme_embeddings(force_update=force_update)
            )
        finally:
            loop.close()
        
        # Check results
        if result.get('success'):
            total = result.get('total', 0)
            processed = result.get('processed', 0)
            failed = result.get('failed', 0)
            skipped = result.get('skipped', 0)
            
            # Update completion status
            update_task_progress(
                task_id,
                status="completed",
                progress=100,
                message=f"Theme embeddings initialization completed: total={total}, processed={processed}, failed={failed}, skipped={skipped}",
                total=total,
                processed=processed,
                failed=failed,
                skipped=skipped
            )
            
            logger.info(f"Task {task_id} completed successfully: {result}")
            return result
        else:
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"Task {task_id} failed: {error_msg}")
            
            update_task_progress(
                task_id,
                status="failed",
                progress=0,
                message=f"Theme embeddings initialization failed: {error_msg}"
            )
            
            return result
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Task {task_id} exception: {error_msg}")
        
        update_task_progress(
            task_id,
            status="failed",
            progress=0,
            message=f"Task exception: {error_msg}"
        )
        
        return {
            "success": False,
            "error": error_msg
        }


@app.task(bind=True, name='fill_theme_descriptions', queue='ai_processing')
def fill_theme_descriptions_task(
    self, 
    force_regenerate: bool = False,
    market: str = None
):
    """
    Celery task: Batch fill theme descriptions using LLM
    
    适用场景：
    - 批量生成主题描述
    - 更新已存在的主题描述
    - 大批量主题处理（使用并发加速）
    
    Args:
        force_regenerate: Whether to force regenerate existing descriptions
        market: Market filter (HK/US/CN), None for all markets
        
    Returns:
        dict: Processing statistics
            - success: bool
            - total: int - Total themes
            - processed: int - Successfully processed
            - failed: int - Failed
    """
    task_id = self.request.id
    
    try:
        logger.info(f"Starting theme descriptions filling task {task_id}, force_regenerate={force_regenerate}, market={market}")
        
        # Import dependencies
        from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise Exception("Database connection failed")
        
        cursor = conn.cursor()
        
        # Build query conditions
        where_clause = ""
        params = []
        
        if market:
            logger.warning(
                f"Market filter requested for fill_theme_descriptions_task "
                f"but investment_theme_embeddings table no longer stores market. "
                f"Ignoring filter: market={market}"
            )
        
        if not force_regenerate:
            # Only fill missing descriptions
            where_clause = f"{where_clause} AND" if where_clause else "WHERE"
            where_clause += " (description IS NULL OR description = '')"
        
        # Get themes to process (从 investment_theme_embeddings 读取)
        cursor.execute(f"""
            SELECT id, theme_id, theme_name, category
            FROM investment_theme_embeddings
            {where_clause}
            ORDER BY theme_name
        """, params)
        
        themes_to_process = cursor.fetchall()
        total = len(themes_to_process)
        
        cursor.close()
        conn.close()
        
        if total == 0:
            logger.info(f"Task {task_id}: No themes to process")
            return {
                "success": True,
                "message": "No themes to process",
                "statistics": {
                    "total": 0,
                    "processed": 0,
                    "failed": 0
                }
            }
        
        # Update initial status
        update_task_progress(
            task_id,
            status="initializing",
            progress=0,
            message=f"Starting to process {total} theme descriptions...",
            total=total,
            processed=0,
            failed=0
        )
        
        # Execute async batch processing
        service = InvestmentThemeEmbeddingService(db_config)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        processed = 0
        failed = 0
        
        try:
            # 🚀 Batch concurrent processing
            batch_size = 5
            
            # Establish persistent database connection
            conn_bg = service.db_manager.get_connection()
            if not conn_bg:
                raise Exception("Cannot establish database connection")
            
            cursor_bg = conn_bg.cursor()
            
            logger.info(f"Starting batch concurrent processing of {total} theme descriptions (batch size: {batch_size})")
            
            for i in range(0, len(themes_to_process), batch_size):
                batch = themes_to_process[i:i+batch_size]
                batch_number = i // batch_size + 1
                total_batches = (len(themes_to_process) + batch_size - 1) // batch_size
                
                # Update progress
                progress = int((i / len(themes_to_process)) * 100)
                update_task_progress(
                    task_id,
                    status="processing",
                    progress=progress,
                    message=f"Processing batch {batch_number}/{total_batches} ({len(batch)} themes)",
                    total=total,
                    processed=processed,
                    failed=failed,
                    current_batch=batch_number,
                    total_batches=total_batches
                )
                
                # Create concurrent task list
                tasks = []
                for row in batch:
                    db_theme_id, theme_id, theme_name, category = row
                    task = service._generate_enriched_theme_text(
                        cursor=cursor_bg,
                        theme_id=theme_id,
                        theme_name=theme_name,
                        category=category,
                        db_theme_id=db_theme_id,
                        force_regenerate=force_regenerate
                    )
                    tasks.append((task, theme_id, theme_name))
                
                # Execute all tasks concurrently
                results = loop.run_until_complete(
                    asyncio.gather(*[t[0] for t in tasks], return_exceptions=True)
                )
                
                # Process results
                for idx, result in enumerate(results):
                    theme_id = tasks[idx][1]
                    theme_name = tasks[idx][2]
                    
                    if isinstance(result, Exception):
                        failed += 1
                        logger.error(f"Description generation failed: {theme_name} - {str(result)}")
                    else:
                        processed += 1
                        logger.debug(f"Description generated: {theme_name}")
                
                # Batch commit
                try:
                    conn_bg.commit()
                    logger.info(f"Batch {batch_number} committed successfully: {len([r for r in results if not isinstance(r, Exception)])}/{len(batch)}")
                except Exception as commit_error:
                    logger.error(f"Failed to commit batch: {str(commit_error)}")
                    conn_bg.rollback()
                
                # Add small delay to avoid API rate limiting
                if i + batch_size < len(themes_to_process):
                    loop.run_until_complete(asyncio.sleep(0.3))
            
            # Close connection
            cursor_bg.close()
            conn_bg.close()
            
        finally:
            loop.close()
        
        # Update completion status
        update_task_progress(
            task_id,
            status="completed",
            progress=100,
            message=f"Batch filling completed: total={total}, processed={processed}, failed={failed}",
            total=total,
            processed=processed,
            failed=failed
        )
        
        logger.info(f"Task {task_id} completed: total={total}, processed={processed}, failed={failed}")
        
        return {
            "success": True,
            "message": f"Batch filling completed",
            "statistics": {
                "total": total,
                "processed": processed,
                "failed": failed
            }
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Task {task_id} exception: {error_msg}")
        
        update_task_progress(
            task_id,
            status="failed",
            progress=0,
            message=f"Task exception: {error_msg}"
        )
        
        return {
            "success": False,
            "error": error_msg
        }


@app.task(bind=True, name='regenerate_all_theme_embeddings', queue='ai_processing')
def regenerate_all_theme_embeddings_task(self, market: str = None):
    """
    Celery task: Regenerate all theme embeddings (descriptions + vectors)
    
    适用场景：
    - 完全重新生成所有主题的描述和向量
    - 模型更新后需要重新生成
    - 大规模数据更新
    
    Args:
        market: Market filter (HK/US/CN), None for all markets
        
    Returns:
        dict: Processing statistics
    """
    task_id = self.request.id
    
    try:
        logger.info(f"Starting regenerate all theme embeddings task {task_id}, market={market}")
        
        # Update initial status
        update_task_progress(
            task_id,
            status="initializing",
            progress=0,
            message="Starting to regenerate all theme embeddings (descriptions + vectors)..."
        )
        
        # Step 1: Fill descriptions (force regenerate)
        logger.info(f"Step 1/2: Filling theme descriptions...")
        update_task_progress(
            task_id,
            status="processing",
            progress=10,
            message="Step 1/2: Generating theme descriptions..."
        )
        
        desc_result = fill_theme_descriptions_task(
            force_regenerate=True,
            market=market
        )
        
        if not desc_result.get('success'):
            raise Exception(f"Description filling failed: {desc_result.get('error')}")
        
        # Step 2: Initialize embeddings (force update)
        logger.info(f"Step 2/2: Initializing theme embeddings...")
        update_task_progress(
            task_id,
            status="processing",
            progress=60,
            message="Step 2/2: Generating theme embeddings..."
        )
        
        embedding_result = initialize_theme_embeddings_task(
            force_update=True
        )
        
        if not embedding_result.get('success'):
            raise Exception(f"Embedding initialization failed: {embedding_result.get('error')}")
        
        # Combine results
        total = embedding_result.get('total', 0)
        processed = embedding_result.get('processed', 0)
        failed = embedding_result.get('failed', 0)
        
        # Update completion status
        update_task_progress(
            task_id,
            status="completed",
            progress=100,
            message=f"All theme embeddings regenerated: total={total}, processed={processed}, failed={failed}",
            total=total,
            processed=processed,
            failed=failed
        )
        
        logger.info(f"Task {task_id} completed successfully")
        
        return {
            "success": True,
            "message": "All theme embeddings regenerated successfully",
            "statistics": {
                "total": total,
                "processed": processed,
                "failed": failed,
                "descriptions": desc_result.get('statistics', {}),
                "embeddings": embedding_result
            }
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Task {task_id} exception: {error_msg}")
        
        update_task_progress(
            task_id,
            status="failed",
            progress=0,
            message=f"Task exception: {error_msg}"
        )
        
        return {
            "success": False,
            "error": error_msg
        }


# fetch_futu_themes_task 已删除
# 原因：移除 futu 爬虫相关功能，改为纯手动主题管理


@app.task(bind=True, name='batch_create_themes', queue='ai_processing')
def batch_create_themes_task(
    self,
    themes_data: dict,
    creator_username: str
):
    """
    Celery task: 批量创建投资主题
    
    适用场景：
    - 从 Excel/CSV 文件批量导入主题
    - 自动生成描述并向量化
    - 创建主题-公司关联
    
    Args:
        themes_data: {
            "AI_Healthcare": {
                "category": "Technology",
                "tickers": ["ABSI", "HIMS", "RXRX"],
                "market": "US"
            },
            ...
        }
        creator_username: 创建者用户名
        
    Returns:
        dict: Processing statistics
            - success: bool
            - total: int - Total themes
            - created: int - Successfully created
            - failed: int - Failed
            - skipped: int - Skipped (already exists)
            - details: List[dict] - Detailed results
    """
    task_id = self.request.id
    total = len(themes_data)  # Store total before try block for exception handler
    
    try:
        logger.info(f"Starting batch create themes task {task_id}, total themes: {total}, user: {creator_username}")
        
        # Import service
        from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        db_manager = DatabaseManager(db_config)
        created = 0
        failed = 0
        skipped = 0
        conflicts = []  # 记录冲突的主题
        details = []
        
        # Update initial status
        update_task_progress(
            task_id,
            status="initializing",
            progress=0,
            message=f"Starting to process {total} themes...",
            total=total,
            created=0,
            failed=0,
            skipped=0
        )
        
        # Execute async batch processing
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # 🚀 Batch concurrent processing
            batch_size = 5  # 每批处理 5 个主题
            
            themes_list = list(themes_data.items())
            total_batches = (len(themes_list) + batch_size - 1) // batch_size
            
            logger.info(f"Starting batch concurrent processing of {total} themes (batch size: {batch_size})")
            
            # 检查已存在的主题
            conn = db_manager.get_connection()
            if not conn:
                raise Exception("Cannot establish database connection")
            
            cursor = conn.cursor()
            
            # 获取已存在的 theme_id
            cursor.execute("""
                SELECT theme_id FROM investment_theme_embeddings
            """)
            existing_theme_ids = set([row[0] for row in cursor.fetchall()])
            
            cursor.close()
            conn.close()
            
            for i in range(0, len(themes_list), batch_size):
                batch = themes_list[i:i+batch_size]
                batch_number = i // batch_size + 1
                
                # Update progress
                progress = int((i / len(themes_list)) * 100)
                update_task_progress(
                    task_id,
                    status="processing",
                    progress=progress,
                    message=f"Processing batch {batch_number}/{total_batches} ({len(batch)} themes)",
                    total=total,
                    created=created,
                    failed=failed,
                    skipped=skipped,
                    conflicts=len(conflicts),
                    current_batch=batch_number,
                    total_batches=total_batches
                )
                
                # Step 1: 并发生成描述
                logger.info(f"Batch {batch_number}: Generating descriptions for {len(batch)} themes")
                
                desc_tasks = []
                theme_names_batch = []
                
                for theme_id, data in batch:
                    # 🔥 修复：从 data 中提取 theme_name 和 category
                    # theme_id 已经由 parser 生成（格式：username_theme_name_category）
                    theme_name = data['theme_name']
                    category = data['category']
                    
                    # 检查是否已存在
                    if theme_id in existing_theme_ids:
                        # 不再直接跳过，而是收集冲突信息
                        logger.warning(f"Theme '{theme_name}' (ID: {theme_id}) conflict detected, collecting conflict info")
                        
                        # 获取现有主题的 tickers
                        try:
                            conflict_conn = db_manager.get_connection()
                            conflict_cursor = conflict_conn.cursor()
                            
                            # 获取现有主题的详细信息
                            conflict_cursor.execute("""
                                SELECT id, theme_name, category, description, "group", created_by
                                FROM investment_theme_embeddings
                                WHERE theme_id = %s
                            """, (theme_id,))
                            
                            existing_row = conflict_cursor.fetchone()
                            if existing_row:
                                db_theme_id, existing_theme_name, existing_category, existing_description, existing_group, existing_created_by = existing_row
                                
                                # 获取现有的 tickers（已经是标准化的，带市场后缀）
                                conflict_cursor.execute("""
                                    SELECT ticker FROM theme_company_associations
                                    WHERE theme_id = %s
                                    ORDER BY ticker
                                """, (theme_id,))
                                
                                existing_tickers = [row[0] for row in conflict_cursor.fetchall()]
                                
                                # 标准化新的 tickers（添加市场后缀）以便正确比较
                                normalized_new_tickers = data.get('tickers', [])
                                if data.get('tickers') and data.get('market'):
                                    try:
                                        from backend.services.theme_company_association_service import ThemeCompanyAssociationService
                                        
                                        association_service = ThemeCompanyAssociationService(db_config)
                                        validation_result = association_service.validate_and_complete_tickers(
                                            data['tickers'],
                                            data['market']
                                        )
                                        
                                        if validation_result.get("valid"):
                                            # 使用标准化后的完整 ticker（带市场后缀）
                                            normalized_new_tickers = [v["full_ticker"] for v in validation_result["valid"]]
                                            logger.debug(f"Normalized new tickers for conflict: {normalized_new_tickers}")
                                    except Exception as e:
                                        logger.warning(f"Failed to normalize new tickers for conflict: {str(e)}")
                                        # 如果标准化失败，使用原始 tickers
                                
                                # 记录冲突信息
                                conflicts.append({
                                    "theme_id": theme_id,
                                    "theme_name": theme_name,
                                    "category": category,
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
                                    "new_data": {
                                        "theme_name": theme_name,
                                        "category": category,
                                        "description": data.get('description', ''),
                                        "group": data.get('group'),
                                        "tickers": normalized_new_tickers,  # 使用标准化后的 tickers
                                        "market": data.get('market')
                                    }
                                })
                                
                                logger.info(f"Conflict recorded: {theme_name} (existing: {len(existing_tickers)} tickers, new: {len(normalized_new_tickers)} tickers)")
                            
                            conflict_cursor.close()
                            conflict_conn.close()
                            
                        except Exception as e:
                            logger.error(f"Failed to collect conflict info for '{theme_name}': {str(e)}")
                        
                        skipped += 1
                        details.append({
                            "theme_name": theme_name,
                            "theme_id": theme_id,
                            "status": "conflict",
                            "reason": "Theme already exists, requires conflict resolution"
                        })
                        continue
                    
                    theme_names_batch.append((theme_name, data, theme_id))

                    # 如果已有 description（如从导出文件导入），跳过 LLM 描述生成
                    if data.get('description'):
                        desc_tasks.append(None)  # 占位符，表示已有描述
                        continue

                    # 验证并获取股票信息
                    stock_symbols = None
                    company_names = None

                    if data.get('tickers') and data.get('market'):
                        try:
                            from backend.services.theme_company_association_service import ThemeCompanyAssociationService

                            association_service = ThemeCompanyAssociationService(db_config)
                            validation_result = association_service.validate_and_complete_tickers(
                                data['tickers'],
                                data['market']
                            )

                            if validation_result.get("valid"):
                                stock_symbols = ", ".join([v["full_ticker"] for v in validation_result["valid"]])
                                company_names = ", ".join([v["company_name"] for v in validation_result["valid"]])
                                logger.debug(f"Validated tickers for '{theme_name}': {stock_symbols}")

                        except Exception as e:
                            logger.warning(f"Failed to validate tickers for '{theme_name}': {str(e)}")

                    # 创建生成描述的任务
                    task = service.generate_theme_description(
                        theme_name=theme_name,
                        category=category,
                        stock_symbols=stock_symbols,
                        company_names=company_names
                    )
                    desc_tasks.append(task)
                
                # 如果当前批次所有主题都被跳过，继续下一批
                if not desc_tasks:
                    logger.info(f"Batch {batch_number}: All themes skipped")
                    continue

                # 分离需要 LLM 生成描述的主题和已有描述的主题
                llm_desc_tasks = [t for t in desc_tasks if t is not None]
                llm_desc_indices = [i for i, t in enumerate(desc_tasks) if t is not None]
                pre_desc_indices = [i for i, t in enumerate(desc_tasks) if t is None]

                # 并发生成描述（仅对需要 LLM 的主题）
                llm_results = []
                if llm_desc_tasks:
                    llm_results = loop.run_until_complete(
                        asyncio.gather(*llm_desc_tasks, return_exceptions=True)
                    )

                # 构建完整的描述结果列表（与 desc_tasks 一一对应）
                descriptions = [None] * len(desc_tasks)
                llm_idx = 0
                for i in range(len(desc_tasks)):
                    if i in pre_desc_indices:
                        # 已有描述，直接使用 data 中的 description
                        _, data, _ = theme_names_batch[i]
                        descriptions[i] = {'success': True, 'description': data.get('description', '')}
                    else:
                        descriptions[i] = llm_results[llm_idx]
                        llm_idx += 1

                # Step 2: 并发创建主题
                logger.info(f"Batch {batch_number}: Creating themes in database")

                create_tasks = []
                create_params = []

                for idx, result in enumerate(descriptions):
                    theme_name, data, theme_id = theme_names_batch[idx]

                    if isinstance(result, Exception):
                        logger.error(f"Description generation failed for '{theme_name}': {str(result)}")
                        failed += 1
                        details.append({
                            "theme_name": theme_name,
                            "status": "failed",
                            "reason": f"Description generation failed: {str(result)}"
                        })
                        continue

                    if not result.get('success'):
                        logger.error(f"Description generation failed for '{theme_name}': {result.get('message')}")
                        failed += 1
                        details.append({
                            "theme_name": theme_name,
                            "status": "failed",
                            "reason": result.get('message', 'Unknown error')
                        })
                        continue

                    description = result.get('description', '')

                    # 获取 category（从 theme_names_batch 中的 data）
                    category = data['category']

                    # 创建主题的任务
                    task = service.create_theme(
                        theme_name=theme_name,
                        category=category,
                        description=description,
                        creator_username=creator_username,
                        theme_id=theme_id,
                        group=data.get('group'),
                        market=data['market'],
                        tickers=data['tickers']
                    )
                    create_tasks.append(task)
                    create_params.append((theme_name, theme_id))
                
                # Execute all theme creation tasks concurrently
                if create_tasks:
                    create_results = loop.run_until_complete(
                        asyncio.gather(*create_tasks, return_exceptions=True)
                    )
                    
                    # Process results
                    for idx, result in enumerate(create_results):
                        theme_name, theme_id = create_params[idx]
                        
                        if isinstance(result, Exception):
                            logger.error(f"Theme creation failed for '{theme_name}': {str(result)}")
                            failed += 1
                            details.append({
                                "theme_name": theme_name,
                                "status": "failed",
                                "reason": f"Theme creation failed: {str(result)}"
                            })
                        elif not result.get('success') and not result.get('theme_saved'):
                            logger.error(f"Theme creation failed for '{theme_name}': {result.get('message')}")
                            failed += 1
                            details.append({
                                "theme_name": theme_name,
                                "status": "failed",
                                "reason": result.get('message', 'Unknown error')
                            })
                        else:
                            created += 1
                            associations_count = result.get('associations_created', 0)
                            logger.info(f"✅ Theme created successfully: '{theme_name}' (ID: {theme_id}, Associations: {associations_count})")
                            
                            # 🔍 Auto-verify tickers for the newly created theme
                            verified_count = 0
                            theme_tickers = theme_names_batch[idx][1].get('tickers', [])
                            
                            if theme_tickers and associations_count > 0:
                                logger.info(f"🔍 Auto-verifying {len(theme_tickers)} tickers for theme '{theme_name}'...")
                                
                                try:
                                    from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
                                    from backend.config.exchange_config import format_ticker_for_eodhd
                                    
                                    # Get ticker associations from database
                                    verify_conn = db_manager.get_connection()
                                    if verify_conn:
                                        verify_cursor = verify_conn.cursor()
                                        
                                        # Get standardized tickers from associations table
                                        verify_cursor.execute("""
                                            SELECT ticker FROM theme_company_associations
                                            WHERE theme_id = %s AND is_verified = false
                                        """, (theme_id,))
                                        
                                        unverified_tickers = [row[0] for row in verify_cursor.fetchall()]
                                        verify_cursor.close()
                                        verify_conn.close()
                                        
                                        if unverified_tickers:
                                            for ticker in unverified_tickers:
                                                try:
                                                    # Parse ticker (e.g., "NVDA.US" -> symbol="NVDA", exchange="US")
                                                    # format_ticker_for_eodhd 返回 "SYMBOL.EXCHANGE" 字符串，需 rsplit 拆分
                                                    formatted_ticker = format_ticker_for_eodhd(ticker)
                                                    if '.' in formatted_ticker:
                                                        symbol, exchange = formatted_ticker.rsplit('.', 1)
                                                    else:
                                                        symbol = formatted_ticker
                                                        exchange = 'US'
                                                    
                                                    # Check if ticker exists in companies table
                                                    check_conn = db_manager.get_connection()
                                                    check_cursor = check_conn.cursor()
                                                    check_cursor.execute("""
                                                        SELECT id, code, name, exchange, primaryticker
                                                        FROM companies
                                                        WHERE primaryticker = %s
                                                    """, (ticker,))
                                                    
                                                    company_exists = check_cursor.fetchone()
                                                    check_cursor.close()
                                                    check_conn.close()
                                                    
                                                    if company_exists:
                                                        # Already in database, mark as verified
                                                        update_conn = db_manager.get_connection()
                                                        update_cursor = update_conn.cursor()
                                                        update_cursor.execute("""
                                                            UPDATE theme_company_associations
                                                            SET is_verified = true
                                                            WHERE theme_id = %s AND ticker = %s
                                                        """, (theme_id, ticker))
                                                        update_conn.commit()
                                                        update_cursor.close()
                                                        update_conn.close()
                                                        verified_count += 1
                                                        logger.debug(f"  ✅ {ticker} verified (already in database)")
                                                    else:
                                                        # Try to fetch from EODHD
                                                        fundamentals_service = EODHDFundamentalsService(db_config)
                                                        fetch_result = loop.run_until_complete(
                                                            fundamentals_service.fetch_and_save_company_fundamentals(
                                                                symbol=symbol,
                                                                exchange=exchange
                                                            )
                                                        )
                                                        
                                                        if fetch_result.get('success'):
                                                            # Mark as verified
                                                            update_conn = db_manager.get_connection()
                                                            update_cursor = update_conn.cursor()
                                                            update_cursor.execute("""
                                                                UPDATE theme_company_associations
                                                                SET is_verified = true
                                                                WHERE theme_id = %s AND ticker = %s
                                                            """, (theme_id, ticker))
                                                            update_conn.commit()
                                                            update_cursor.close()
                                                            update_conn.close()
                                                            verified_count += 1
                                                            logger.debug(f"  ✅ {ticker} verified (fetched from EODHD)")
                                                        else:
                                                            logger.warning(f"  ⚠️ {ticker} could not be verified (not found in EODHD)")
                                                    
                                                except Exception as verify_error:
                                                    logger.warning(f"  ❌ Failed to verify {ticker}: {str(verify_error)}")
                                                    continue
                                            
                                            logger.info(f"🔍 Auto-verification completed for '{theme_name}': {verified_count}/{len(unverified_tickers)} tickers verified")
                                        else:
                                            logger.debug(f"No unverified tickers for theme '{theme_name}'")
                                    
                                except Exception as e:
                                    logger.warning(f"Auto-verification failed for theme '{theme_name}': {str(e)}")
                            
                            details.append({
                                "theme_name": theme_name,
                                "theme_id": theme_id,
                                "status": "success",
                                "associations_created": associations_count,
                                "verified_count": verified_count,
                                "embedding_created": result.get('embedding_created', False)
                            })
                
                # Update progress after batch
                progress = int(((i + len(batch)) / len(themes_list)) * 100)
                update_task_progress(
                    task_id,
                    status="processing",
                    progress=progress,
                    message=f"Batch {batch_number}/{total_batches} completed",
                    total=total,
                    created=created,
                    failed=failed,
                    skipped=skipped,
                    current_batch=batch_number,
                    total_batches=total_batches
                )
                
                # Add small delay to avoid API rate limiting
                if i + batch_size < len(themes_list):
                    logger.debug(f"Waiting 0.5s before next batch...")
                    loop.run_until_complete(asyncio.sleep(0.5))
            
        finally:
            loop.close()
        
        # Update completion status
        update_task_progress(
            task_id,
            status="completed",
            progress=100,
            message=f"Batch creation completed: total={total}, created={created}, failed={failed}, skipped={skipped}",
            total=total,
            created=created,
            failed=failed,
            skipped=skipped
        )
        
        logger.info(f"Task {task_id} completed: total={total}, created={created}, failed={failed}, skipped={skipped}, conflicts={len(conflicts)}")
        
        created_theme_ids = [
            d['theme_id'] for d in details
            if d.get('status') == 'success' and d.get('theme_id')
        ]

        # 为新建主题自动打 'Theme Report' 个人标签，使其纳入 Theme Report 生成范围。
        # Theme Report 通过 theme_tags JOIN 筛选 tag_name='Theme Report' AND user_id=<用户>
        # （见 theme_report_service.py:446-449 的 _get_tagged_themes）。CSV 导入的主题此前
        # 缺失此标签，导致 generate_all_theme_reports 找不到目标、报告为空。
        # 唯一调用方为 CSV 上传（investment_themes.py:2649），默认打标符合该路径语义；
        # ON CONFLICT 幂等，重复导入不重复；失败仅告警，不影响已创建的主题与返回值。
        if created_theme_ids:
            try:
                tag_conn = db_manager.get_connection()
                if tag_conn:
                    tag_cursor = tag_conn.cursor()
                    tag_cursor.executemany(
                        """
                        INSERT INTO theme_tags (theme_id, tag_name, user_id, created_by, created_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (theme_id, tag_name, user_id) DO NOTHING
                        """,
                        [(_tid, "Theme Report", creator_username, creator_username)
                         for _tid in created_theme_ids],
                    )
                    tag_conn.commit()
                    tag_cursor.close()
                    tag_conn.close()
                    logger.info(
                        f"🏷️ 已为 {len(created_theme_ids)} 个新主题自动打 'Theme Report' 标签 "
                        f"(user={creator_username})"
                    )
            except Exception as tag_err:
                logger.warning(f"⚠️ 自动打 'Theme Report' 标签失败（不影响主题创建）: {tag_err}")

        return {
            "success": True,
            "message": f"Batch creation completed",
            "statistics": {
                "total": total,
                "created": created,
                "failed": failed,
                "skipped": skipped,
                "conflicts": len(conflicts)
            },
            "conflicts": conflicts,  # 返回冲突列表
            "details": details,
            "theme_ids": created_theme_ids,
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Task {task_id} exception: {error_msg}")
        
        update_task_progress(
            task_id,
            status="failed",
            progress=0,
            message=f"Task exception: {error_msg}"
        )
        
        return {
            "success": False,
            "error": error_msg,
            "statistics": {
                "total": total,
                "created": 0,
                "failed": 0,
                "skipped": 0
            }
        }


@app.task(bind=True, name='batch_resolve_conflicts', queue='ai_processing')
def batch_resolve_conflicts_task(
    self,
    conflict_resolutions: dict,
    operator_username: str
):
    """
    Celery task: 批量解决主题冲突
    
    Args:
        conflict_resolutions: 冲突解决方案字典
            格式: {
                theme_id: {
                    "theme_name": str,
                    "category": str,
                    "description": str,
                    "selected_tickers": [str],  # 用户选择的 tickers（已标准化）
                    "group": str (optional)
                }
            }
        operator_username: 操作者用户名
    
    Returns:
        dict: {
            "success": bool,
            "message": str,
            "statistics": {
                "total": int,
                "success": int,
                "failed": int
            },
            "details": [...]
        }
    """
    task_id = self.request.id
    logger.info(f"Starting batch resolve conflicts task {task_id}, total conflicts: {len(conflict_resolutions)}, user: {operator_username}")
    
    try:
        from backend.database.db_manager import DatabaseManager
        from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        total = len(conflict_resolutions)
        success_count = 0
        failed_count = 0
        skipped_count = 0  # 新增：跳过计数（ticker完全相同，无需处理）
        details = []
        
        # Update initial status
        update_task_progress(
            task_id,
            status="initializing",
            progress=0,
            message=f"Initializing batch conflict resolution for {total} themes...",
            total=total,
            success=0,
            failed=0,
            skipped=0
        )
        
        # 批量并行处理冲突
        batch_size = 5  # 每批处理5个，避免过多并发
        conflicts_list = list(conflict_resolutions.items())
        total_batches = (len(conflicts_list) + batch_size - 1) // batch_size
        
        logger.info(f"Processing {total} conflicts in {total_batches} batches (batch size: {batch_size})")
        
        service = InvestmentThemeEmbeddingService(db_config)
        
        for batch_num in range(total_batches):
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, total)
            batch = conflicts_list[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches}: conflicts {batch_start + 1}-{batch_end}")
            
            # 并行处理当前批次
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # 定义异步处理单个冲突的函数
                async def process_single_conflict(theme_id: str, resolution: dict, conflict_index: int):
                    try:
                        theme_name = resolution['theme_name']
                        category = resolution['category']
                        description = resolution['description']
                        selected_tickers = resolution['selected_tickers']
                        group = resolution.get('group')
                        
                        logger.info(f"[Batch {batch_num + 1}] Processing conflict {conflict_index + 1}/{total}: {theme_id}, {len(selected_tickers)} tickers")
                        
                        # 🔥 优化：检查 tickers 是否有变化
                        # 获取现有主题的 tickers
                        conn = db_manager.get_connection()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute("""
                                SELECT ticker FROM theme_company_associations
                                WHERE theme_id = %s
                                ORDER BY ticker
                            """, (theme_id,))
                            
                            existing_tickers = [row[0] for row in cursor.fetchall()]
                            cursor.close()
                            conn.close()
                            
                            # 比较集合：如果完全相同，说明所有ticker都是"重复"，无需重新生成
                            existing_set = set(existing_tickers)
                            selected_set = set(selected_tickers)
                            
                            if existing_set == selected_set:
                                logger.info(f"⏭️ Skipping {theme_id}: All tickers are duplicates, no changes needed (saved LLM + vectorization)")
                                # 直接返回成功，不需要任何LLM或向量化操作
                                return {
                                    "success": True,
                                    "theme_id": theme_id,
                                    "theme_name": theme_name,
                                    "selected_tickers": selected_tickers,
                                    "skipped": True,
                                    "reason": "no_ticker_changes"
                                }
                        
                        # 🔥 如果有变化，才进行LLM生成和向量化
                        logger.info(f"🔄 {theme_id}: Tickers changed, regenerating description and vector...")
                        
                        # 获取公司名称
                        stock_symbols = ", ".join(selected_tickers)
                        company_names = None
                        
                        if len(selected_tickers) > 0:
                            try:
                                conn = db_manager.get_connection()
                                if conn:
                                    cursor = conn.cursor()
                                    placeholders = ','.join(['%s'] * len(selected_tickers))
                                    cursor.execute(f"""
                                        SELECT ticker, company_name 
                                        FROM company_profiles 
                                        WHERE ticker IN ({placeholders})
                                    """, selected_tickers)
                                    
                                    companies = {row[0]: row[1] for row in cursor.fetchall()}
                                    company_names = ", ".join([companies.get(t, t) for t in selected_tickers])
                                    
                                    cursor.close()
                                    conn.close()
                            except Exception as e:
                                logger.debug(f"Failed to fetch company names for {theme_id}: {str(e)}")
                        
                        # 并行生成描述和更新向量
                        desc_result = await service.generate_theme_description(
                            theme_name=theme_name,
                            category=category,
                            stock_symbols=stock_symbols,
                            company_names=company_names
                        )
                        
                        if desc_result.get('success'):
                            new_description = desc_result.get('description', description)
                        else:
                            logger.warning(f"Failed to generate description for {theme_id}, using existing")
                            new_description = description
                        
                        # 更新主题描述和向量
                        update_result = await service.update_theme_description(
                            theme_id=theme_id,
                            new_description=new_description,
                            editor_username=operator_username,
                            new_group=group,
                            version=None
                        )
                        
                        if not update_result.get('description_saved'):
                            raise Exception(f"Failed to update theme: {update_result.get('message')}")
                        
                        # 返回结果，稍后批量更新数据库关联
                        return {
                            "success": True,
                            "theme_id": theme_id,
                            "theme_name": theme_name,
                            "selected_tickers": selected_tickers
                        }
                        
                    except Exception as e:
                        return {
                            "success": False,
                            "theme_id": theme_id,
                            "theme_name": resolution.get('theme_name', 'Unknown'),
                            "error": str(e)
                        }
                
                # 并行执行当前批次的所有冲突
                tasks = [
                    process_single_conflict(theme_id, resolution, batch_start + idx)
                    for idx, (theme_id, resolution) in enumerate(batch)
                ]
                
                batch_results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                
                # 批量更新数据库关联（同步操作）
                for idx, result in enumerate(batch_results):
                    conflict_index = batch_start + idx
                    
                    if isinstance(result, Exception):
                        failed_count += 1
                        logger.error(f"Batch processing exception: {str(result)}")
                        details.append({
                            "theme_id": "Unknown",
                            "theme_name": "Unknown",
                            "status": "failed",
                            "message": str(result)
                        })
                        continue
                    
                    if result.get('success'):
                        theme_id = result['theme_id']
                        theme_name = result['theme_name']
                        selected_tickers = result['selected_tickers']
                        
                        # 🔥 检查是否跳过（ticker完全相同）
                        if result.get('skipped'):
                            skipped_count += 1
                            details.append({
                                "theme_id": theme_id,
                                "theme_name": theme_name,
                                "status": "skipped",
                                "message": "All tickers are duplicates, no changes needed (saved LLM + vectorization)"
                            })
                            logger.info(f"⏭️ Skipped {theme_id}: no ticker changes")
                            continue
                        
                        try:
                            # 删除旧关联并创建新关联
                            conn = db_manager.get_connection()
                            if not conn:
                                raise Exception("Cannot establish database connection")
                            
                            cursor = conn.cursor()
                            
                            # 获取旧的tickers用于对比（找出新增的）
                            cursor.execute("""
                                SELECT ticker FROM theme_company_associations
                                WHERE theme_id = %s
                            """, (theme_id,))
                            old_tickers = set([row[0] for row in cursor.fetchall()])
                            
                            # 删除旧关联
                            cursor.execute("""
                                DELETE FROM theme_company_associations
                                WHERE theme_id = %s
                            """, (theme_id,))
                            old_count = cursor.rowcount
                            
                            # 创建新关联
                            new_count = 0
                            newly_added_tickers = []  # 记录新增的ticker
                            for ticker in selected_tickers:
                                if '.' in ticker:
                                    symbol, exchange_suffix = ticker.rsplit('.', 1)
                                    market_map = {'US': 'US', 'HK': 'HK', 'SHG': 'CN', 'SHE': 'CN'}
                                    ticker_market = market_map.get(exchange_suffix, 'US')
                                else:
                                    ticker_market = 'US'
                                
                                cursor.execute("""
                                    INSERT INTO theme_company_associations 
                                    (theme_id, ticker, market, created_at)
                                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                                    ON CONFLICT (theme_id, ticker) DO NOTHING
                                """, (theme_id, ticker, ticker_market))
                                new_count += cursor.rowcount
                                
                                # 记录新增的ticker
                                if ticker not in old_tickers:
                                    newly_added_tickers.append(ticker)
                            
                            conn.commit()
                            cursor.close()
                            conn.close()
                            
                            # 🔥 自动验证所有勾选的 tickers（不只是新增的，所有勾选的未验证ticker都要验证）
                            verified_count = 0
                            if len(selected_tickers) > 0:
                                logger.info(f"🔍 Auto-verifying {len(selected_tickers)} selected tickers for {theme_id}...")
                                try:
                                    from backend.database.db_manager import DatabaseManager
                                    from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
                                    from backend.config.exchange_config import format_ticker_for_eodhd
                                    
                                    # 先获取未验证的ticker列表
                                    verify_check_conn = db_manager.get_connection()
                                    verify_check_cursor = verify_check_conn.cursor()
                                    verify_check_cursor.execute("""
                                        SELECT ticker FROM theme_company_associations
                                        WHERE theme_id = %s AND (is_verified IS NULL OR is_verified = false)
                                    """, (theme_id,))
                                    unverified_tickers = set([row[0] for row in verify_check_cursor.fetchall()])
                                    verify_check_cursor.close()
                                    verify_check_conn.close()
                                    
                                    logger.info(f"  Found {len(unverified_tickers)} unverified tickers to validate")
                                    
                                    for ticker in selected_tickers:
                                        # 只验证未验证的ticker
                                        if ticker not in unverified_tickers:
                                            logger.debug(f"  ⏭️ {ticker} already verified, skipping")
                                            continue
                                        try:
                                            # 检查ticker是否在companies表中
                                            verify_conn = db_manager.get_connection()
                                            verify_cursor = verify_conn.cursor()
                                            
                                            verify_cursor.execute("""
                                                SELECT id, code, name, exchange, primaryticker
                                                FROM companies
                                                WHERE UPPER(primaryticker) = UPPER(%s)
                                                LIMIT 1
                                            """, (ticker,))
                                            
                                            company_result = verify_cursor.fetchone()
                                            
                                            if company_result:
                                                # 在数据库中找到，更新is_verified
                                                verify_cursor.execute("""
                                                    UPDATE theme_company_associations
                                                    SET is_verified = TRUE,
                                                        company_name = %s,
                                                        updated_at = NOW()
                                                    WHERE theme_id = %s AND ticker = %s
                                                """, (company_result[2], theme_id, ticker))
                                                verify_conn.commit()
                                                verified_count += 1
                                                logger.debug(f"✅ Ticker {ticker} verified (found in DB)")
                                            else:
                                                # 不在数据库中，尝试从EODHD获取
                                                logger.debug(f"🔍 Ticker {ticker} not in DB, fetching from EODHD...")
                                                
                                                formatted_ticker = format_ticker_for_eodhd(ticker)
                                                if '.' in formatted_ticker:
                                                    symbol, exchange = formatted_ticker.rsplit('.', 1)
                                                else:
                                                    symbol = formatted_ticker
                                                    exchange = 'US'
                                                
                                                fundamentals_service = EODHDFundamentalsService()
                                                full_data = fundamentals_service.fetch_company_full_data(symbol, exchange)
                                                
                                                if full_data:
                                                    # 保存到数据库
                                                    company_data = {
                                                        'symbol': symbol,
                                                        'exchange': exchange,
                                                        'general_data': full_data.get('General', full_data),
                                                        'address_data': full_data.get('General', full_data).get('AddressData') if full_data.get('General') else None,
                                                        'officers_data': fundamentals_service._extract_officers_list(full_data.get('General', full_data).get('Officers', {})) if full_data.get('General') else [],
                                                        'listings_data': full_data.get('General', full_data).get('Listings') if full_data.get('General') else None,
                                                        'full_data': full_data
                                                    }
                                                    
                                                    fundamentals_service.batch_save_companies_data([company_data])
                                                    
                                                    # 更新is_verified
                                                    general_data = full_data.get('General', full_data)
                                                    company_name = general_data.get('Name', symbol)
                                                    
                                                    verify_cursor.execute("""
                                                        UPDATE theme_company_associations
                                                        SET is_verified = TRUE,
                                                            company_name = %s,
                                                            updated_at = NOW()
                                                        WHERE theme_id = %s AND ticker = %s
                                                    """, (company_name, theme_id, ticker))
                                                    verify_conn.commit()
                                                    verified_count += 1
                                                    logger.info(f"✅ Ticker {ticker} verified and fetched from EODHD")
                                                else:
                                                    logger.warning(f"⚠️ Ticker {ticker} not found in EODHD")
                                            
                                            verify_cursor.close()
                                            verify_conn.close()
                                            
                                        except Exception as verify_error:
                                            logger.warning(f"Failed to verify ticker {ticker}: {str(verify_error)}")
                                            continue
                                    
                                    logger.info(f"🔍 Auto-verification completed: {verified_count}/{len(unverified_tickers)} tickers verified")
                                    
                                except Exception as e:
                                    logger.warning(f"Auto-verification failed: {str(e)}")
                            
                            success_count += 1
                            details.append({
                                "theme_id": theme_id,
                                "theme_name": theme_name,
                                "status": "success",
                                "message": f"Updated: deleted {old_count} old associations, created {new_count} new associations, verified {verified_count} tickers"
                            })
                            
                            logger.info(f"✅ Successfully resolved conflict for {theme_id}: {old_count} deleted, {new_count} created, {verified_count} verified")
                            
                        except Exception as e:
                            failed_count += 1
                            error_msg = str(e)
                            details.append({
                                "theme_id": theme_id,
                                "theme_name": theme_name,
                                "status": "failed",
                                "message": f"DB update failed: {error_msg}"
                            })
                            logger.error(f"Failed to update DB for {theme_id}: {error_msg}")
                    else:
                        failed_count += 1
                        details.append({
                            "theme_id": result.get('theme_id', 'Unknown'),
                            "theme_name": result.get('theme_name', 'Unknown'),
                            "status": "failed",
                            "message": result.get('error', 'Unknown error')
                        })
                        logger.error(f"Failed to resolve conflict for {result.get('theme_id')}: {result.get('error')}")
                    
                    # 每处理一个冲突就更新进度
                    progress = int(((conflict_index + 1) / total) * 100)
                    update_task_progress(
                        task_id,
                        status="processing",
                        progress=progress,
                        message=f"Processed {conflict_index + 1}/{total} conflicts",
                        total=total,
                        success=success_count,
                        failed=failed_count,
                        skipped=skipped_count
                    )
                
            finally:
                loop.close()
            
            logger.info(f"Batch {batch_num + 1}/{total_batches} completed: {success_count} success, {failed_count} failed, {skipped_count} skipped (no changes) so far")
        
        # Update completion status
        update_task_progress(
            task_id,
            status="completed",
            progress=100,
            message=f"Batch conflict resolution completed: {success_count} success, {failed_count} failed, {skipped_count} skipped (no changes)",
            total=total,
            success=success_count,
            failed=failed_count,
            skipped=skipped_count
        )
        
        logger.info(f"Task {task_id} completed: total={total}, success={success_count}, failed={failed_count}, skipped={skipped_count}")
        
        return {
            "success": True,
            "message": f"Batch conflict resolution completed",
            "statistics": {
                "total": total,
                "success": success_count,
                "failed": failed_count,
                "skipped": skipped_count
            },
            "details": details
        }
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Task {task_id} exception: {error_msg}")
        
        update_task_progress(
            task_id,
            status="failed",
            progress=0,
            message=f"Task exception: {error_msg}"
        )
        
        return {
            "success": False,
            "error": error_msg,
            "statistics": {
                "total": total,
                "created": 0,
                "failed": 0,
                "skipped": 0
            }
        }

