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
            where_clause = "WHERE market = %s"
            params.append(market)
        
        if not force_regenerate:
            # Only fill missing descriptions
            where_clause = f"{where_clause} AND" if where_clause else "WHERE"
            where_clause += " (description IS NULL OR description = '')"
        
        # Get themes to process (从 investment_theme_embeddings 读取)
        cursor.execute(f"""
            SELECT id, theme_id, theme_name, market
            FROM investment_theme_embeddings
            {where_clause}
            ORDER BY market, theme_name
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
                    db_theme_id, theme_id, theme_name, market = row
                    task = service._generate_enriched_theme_text(
                        cursor=cursor_bg,
                        theme_id=theme_id,
                        theme_name=theme_name,
                        market=market,
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

