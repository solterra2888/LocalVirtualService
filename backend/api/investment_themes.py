# -*- coding: utf-8 -*-
"""
投资主题向量化管理API
提供主题向量初始化、匹配、统计等功能
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

router = APIRouter(prefix="/investment-themes", tags=["Investment Themes"])
logger = get_logger("investment_themes_api")


# ==================== Request/Response Models ====================

class ThemeMatchRequest(BaseModel):
    """主题匹配请求"""
    news_text: str = Field(..., description="新闻文本（标题+内容）")
    news_id: Optional[int] = Field(None, description="新闻ID（用于保存匹配结果）")
    market: Optional[str] = Field(None, description="市场过滤（HK/US/CN）")
    top_k: int = Field(5, ge=1, le=20, description="返回Top-K个主题")
    min_similarity: float = Field(0.6, ge=0.0, le=1.0, description="最小相似度阈值")
    save_to_db: bool = Field(False, description="是否保存匹配结果到数据库")


class ThemeMatchResponse(BaseModel):
    """主题匹配响应"""
    theme_id: str
    theme_name: str
    market: str
    similarity: float
    confidence: float
    rank: int


class InitializeThemesRequest(BaseModel):
    """初始化主题向量请求"""
    force_update: bool = Field(False, description="是否强制更新已存在的向量")


class UpdateThemeRequest(BaseModel):
    """更新单个主题向量请求"""
    theme_id: str = Field(..., description="主题ID")
    theme_name: str = Field(..., description="主题名称")
    market: str = Field(..., description="市场代码（HK/US/CN）")


# ==================== API Endpoints ====================

@router.post("/match", response_model=List[ThemeMatchResponse])
async def match_news_to_themes(request: ThemeMatchRequest):
    """
    将新闻匹配到投资主题（基于向量相似度）
    
    使用场景：
    - 新闻分析后自动匹配相关投资主题
    - 前端实时预览新闻可能匹配的主题
    
    示例：
    ```
    POST /api/investment-themes/match
    {
        "news_text": "特斯拉发布新款电动车，配备先进自动驾驶功能",
        "market": "US",
        "top_k": 3,
        "min_similarity": 0.6
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"收到主题匹配请求",
            news_id=request.news_id,
            market=request.market,
            top_k=request.top_k
        )
        
        matches = await service.match_news_to_themes(
            news_text=request.news_text,
            news_id=request.news_id,
            market=request.market,
            top_k=request.top_k,
            min_similarity=request.min_similarity,
            save_to_db=request.save_to_db
        )
        
        logger.info(f"主题匹配完成，找到 {len(matches)} 个匹配")
        
        return matches
        
    except Exception as e:
        logger.error(f"主题匹配失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"主题匹配失败: {str(e)}")


@router.post("/initialize")
async def initialize_theme_embeddings(
    request: InitializeThemesRequest,
    background_tasks: BackgroundTasks
):
    """
    初始化所有投资主题的向量
    
    说明：
    - 从 futu_investment_themes 表读取主题
    - 调用Qwen Embedding API生成向量
    - 存储到 investment_theme_embeddings 表
    - 支持后台任务执行（大量主题时推荐）
    
    注意：
    - 首次运行需要几分钟时间
    - force_update=True 将重新生成所有向量
    
    示例：
    ```
    POST /api/investment-themes/initialize
    {
        "force_update": false
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info("开始初始化投资主题向量", force_update=request.force_update)
        
        # 执行初始化
        result = await service.initialize_all_theme_embeddings(
            force_update=request.force_update
        )
        
        if result.get('success'):
            logger.info(
                f"主题向量初始化完成",
                total=result.get('total'),
                processed=result.get('processed'),
                failed=result.get('failed')
            )
            
            return {
                "success": True,
                "message": "主题向量初始化完成",
                "statistics": result
            }
        else:
            raise HTTPException(
                status_code=500,
                detail=f"初始化失败: {result.get('error', 'Unknown error')}"
            )
            
    except Exception as e:
        logger.error(f"初始化主题向量失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"初始化失败: {str(e)}")


@router.get("/statistics")
async def get_theme_statistics():
    """
    获取投资主题向量统计信息
    
    返回信息包括：
    - 主题总数
    - 各市场主题数量分布
    - 新闻主题匹配统计
    - Embedding模型信息
    
    示例：
    ```
    GET /api/investment-themes/statistics
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        stats = service.get_theme_statistics()
        
        if not stats:
            raise HTTPException(status_code=404, detail="未找到统计信息")
        
        return {
            "success": True,
            "statistics": stats
        }
        
    except Exception as e:
        logger.error(f"获取统计信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取统计信息失败: {str(e)}")


@router.put("/update-theme")
async def update_theme_embedding(request: UpdateThemeRequest):
    """
    更新单个主题的向量
    
    使用场景：
    - 新增主题后生成向量
    - 主题名称修改后更新向量
    
    示例：
    ```
    PUT /api/investment-themes/update-theme
    {
        "theme_id": "EV_US_001",
        "theme_name": "Electric Vehicles",
        "market": "US"
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(f"更新主题向量: {request.theme_id} - {request.theme_name}")
        
        success = await service.update_single_theme_embedding(
            theme_id=request.theme_id,
            theme_name=request.theme_name,
            market=request.market
        )
        
        if success:
            return {
                "success": True,
                "message": f"主题向量更新成功: {request.theme_name}"
            }
        else:
            raise HTTPException(status_code=500, detail="更新失败")
            
    except Exception as e:
        logger.error(f"更新主题向量失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.get("/news/{news_id}/themes")
async def get_news_themes(news_id: int):
    """
    获取某条新闻匹配的投资主题
    
    从 news_theme_matches 表查询已保存的匹配结果
    
    示例：
    ```
    GET /api/investment-themes/news/123/themes
    ```
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                theme_id,
                theme_name,
                market,
                similarity_score,
                match_rank,
                match_method,
                matched_at
            FROM news_theme_matches
            WHERE news_id = %s
            ORDER BY match_rank ASC
        """, (news_id,))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'theme_id': row[0],
                'theme_name': row[1],
                'market': row[2],
                'similarity': float(row[3]),
                'rank': row[4],
                'match_method': row[5],
                'matched_at': row[6].isoformat() if row[6] else None
            })
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "news_id": news_id,
            "themes": results,
            "count": len(results)
        }
        
    except Exception as e:
        logger.error(f"获取新闻主题失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@router.get("/themes")
async def list_all_themes(
    market: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    列出所有投资主题
    
    参数：
    - market: 市场过滤（HK/US/CN）
    - limit: 每页数量
    - offset: 偏移量
    
    示例：
    ```
    GET /api/investment-themes/themes?market=US&limit=20
    ```
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 构建查询
        where_clause = ""
        params = []
        
        if market:
            where_clause = "WHERE market = %s"
            params.append(market)
        
        # 获取总数
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM investment_theme_embeddings 
            {where_clause}
        """, params)
        total_count = cursor.fetchone()[0]
        
        # 获取列表
        params.extend([limit, offset])
        cursor.execute(f"""
            SELECT 
                theme_id,
                theme_name,
                market,
                embedding_model,
                created_at,
                updated_at
            FROM investment_theme_embeddings
            {where_clause}
            ORDER BY market, theme_name
            LIMIT %s OFFSET %s
        """, params)
        
        themes = []
        for row in cursor.fetchall():
            themes.append({
                'theme_id': row[0],
                'theme_name': row[1],
                'market': row[2],
                'embedding_model': row[3],
                'created_at': row[4].isoformat() if row[4] else None,
                'updated_at': row[5].isoformat() if row[5] else None
            })
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "themes": themes,
            "total": total_count,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"获取主题列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")

