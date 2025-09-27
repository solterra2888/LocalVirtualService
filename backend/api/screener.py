"""
Finviz Screener API
提供股票筛选器功能的RESTful API接口
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import Optional, Dict, Any, List
from datetime import datetime, date
import logging

from ..database.db_manager import DatabaseManager
from ..utils.database_config import get_database_config
from ..schemas.user import TokenData
from ..auth.jwt_utils import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/screener", tags=["screener"])

def get_db_connection():
    """获取数据库连接"""
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    return db_manager.get_connection()


@router.get("/data")
async def get_screener_data(
    view: str = Query("overview", description="数据视图: overview, valuation, financial, ownership, performance, technical"),
    page: int = Query(1, ge=1, description="页码"),
    pageSize: int = Query(20, ge=1, le=100, description="每页数量"),
    # 筛选器参数
    exchange: Optional[str] = Query(None, description="交易所"),
    sector: Optional[str] = Query(None, description="行业板块"),
    industry: Optional[str] = Query(None, description="细分行业"),
    country: Optional[str] = Query(None, description="国家"),
    marketCap: Optional[str] = Query(None, description="市值范围"),
    price: Optional[str] = Query(None, description="价格范围"),
    # 排序参数
    sortBy: Optional[str] = Query(None, description="排序字段"),
    sortOrder: str = Query("asc", description="排序方向: asc, desc"),
    current_user: TokenData = Depends(get_current_user)
):
    """获取筛选器数据"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 构建SQL查询
        base_query = """
        SELECT 
            id, ticker, company, index_name, sector, industry, country, exchange,
            market_cap, pe_ratio, forward_pe, peg, ps_ratio, pb_ratio, pcash_ratio, pfree_cash_flow,
            dividend, eps_ttm, eps_growth_this_year, eps_growth_next_year, eps_growth_past_5_years,
            eps_growth_next_5_years, sales_growth_past_5_years, shares_outstanding, shares_float,
            insider_ownership, insider_transactions, institutional_ownership, institutional_transactions,
            short_float, short_ratio, return_on_assets, return_on_equity, return_on_invested_capital,
            current_ratio, quick_ratio, lt_debt_equity, total_debt_equity, gross_margin, operating_margin,
            profit_margin, performance_week, performance_month, performance_quarter, performance_half_year,
            performance_ytd, performance_year, performance_3_years, performance_5_years, performance_10_years,
            beta, average_true_range, volatility_week, volatility_month, sma_20_day, sma_50_day, sma_200_day,
            week_52_high, week_52_low, week_52_range, rsi_14, earnings_date, change_from_open, gap,
            average_volume, relative_volume, volume, price, change_percent, created_at, updated_at, snapshot_date
        FROM finviz_screener
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener)
        """
        
        # 构建筛选条件
        where_conditions = []
        params = []
        
        if exchange:
            where_conditions.append("exchange = %s")
            params.append(exchange)
            
        if sector:
            where_conditions.append("sector = %s")
            params.append(sector)
            
        if industry:
            where_conditions.append("industry = %s")
            params.append(industry)
            
        if country:
            where_conditions.append("country = %s")
            params.append(country)
            
        # 市值筛选
        if marketCap:
            if marketCap == "nano":
                where_conditions.append("market_cap < 50000000")
            elif marketCap == "micro":
                where_conditions.append("market_cap >= 50000000 AND market_cap < 300000000")
            elif marketCap == "small":
                where_conditions.append("market_cap >= 300000000 AND market_cap < 2000000000")
            elif marketCap == "mid":
                where_conditions.append("market_cap >= 2000000000 AND market_cap < 10000000000")
            elif marketCap == "large":
                where_conditions.append("market_cap >= 10000000000 AND market_cap < 100000000000")
            elif marketCap == "mega":
                where_conditions.append("market_cap >= 100000000000")
                
        # 价格筛选
        if price:
            if price == "under1":
                where_conditions.append("price < 1")
            elif price == "1to5":
                where_conditions.append("price >= 1 AND price < 5")
            elif price == "5to10":
                where_conditions.append("price >= 5 AND price < 10")
            elif price == "10to20":
                where_conditions.append("price >= 10 AND price < 20")
            elif price == "20to50":
                where_conditions.append("price >= 20 AND price < 50")
            elif price == "over50":
                where_conditions.append("price >= 50")
        
        # 添加筛选条件到查询
        if where_conditions:
            base_query += " AND " + " AND ".join(where_conditions)
            
        # 添加排序
        if sortBy:
            order_direction = "DESC" if sortOrder == "desc" else "ASC"
            base_query += f" ORDER BY {sortBy} {order_direction}"
        else:
            base_query += " ORDER BY market_cap DESC NULLS LAST"
            
        # 获取总数
        count_query = f"SELECT COUNT(*) FROM ({base_query}) as filtered_data"
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # 添加分页
        offset = (page - 1) * pageSize
        paginated_query = base_query + f" LIMIT %s OFFSET %s"
        params.extend([pageSize, offset])
        
        # 执行查询
        cursor.execute(paginated_query, params)
        rows = cursor.fetchall()
        
        # 获取列名
        column_names = [desc[0] for desc in cursor.description]
        
        # 转换为字典列表
        data = []
        for row in rows:
            row_dict = dict(zip(column_names, row))
            # 处理日期字段
            if row_dict.get('earnings_date'):
                row_dict['earnings_date'] = row_dict['earnings_date'].isoformat() if isinstance(row_dict['earnings_date'], datetime) else str(row_dict['earnings_date'])
            if row_dict.get('created_at'):
                row_dict['created_at'] = row_dict['created_at'].isoformat() if isinstance(row_dict['created_at'], datetime) else str(row_dict['created_at'])
            if row_dict.get('updated_at'):
                row_dict['updated_at'] = row_dict['updated_at'].isoformat() if isinstance(row_dict['updated_at'], datetime) else str(row_dict['updated_at'])
            if row_dict.get('snapshot_date'):
                row_dict['snapshot_date'] = row_dict['snapshot_date'].isoformat() if isinstance(row_dict['snapshot_date'], date) else str(row_dict['snapshot_date'])
            data.append(row_dict)
        
        cursor.close()
        conn.close()
        
        logger.info(f"获取筛选器数据: {view} 视图, 第{page}页, 共{total_count}条记录")
        
        return {
            "data": data,
            "total": total_count,
            "page": page,
            "pageSize": pageSize,
            "hasMore": page * pageSize < total_count
        }
        
    except Exception as e:
        logger.error(f"获取筛选器数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取筛选器数据失败: {str(e)}")


@router.get("/stats")
async def get_screener_stats(
    exchange: Optional[str] = Query(None),
    sector: Optional[str] = Query(None),
    industry: Optional[str] = Query(None),
    country: Optional[str] = Query(None),
    marketCap: Optional[str] = Query(None),
    price: Optional[str] = Query(None),
    current_user: TokenData = Depends(get_current_user)
):
    """获取筛选器统计信息"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 构建基础筛选条件（与get_screener_data相同的逻辑）
        where_conditions = ["snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener)"]
        params = []
        
        if exchange:
            where_conditions.append("exchange = %s")
            params.append(exchange)
        if sector:
            where_conditions.append("sector = %s")
            params.append(sector)
        if industry:
            where_conditions.append("industry = %s")
            params.append(industry)
        if country:
            where_conditions.append("country = %s")
            params.append(country)
            
        where_clause = " WHERE " + " AND ".join(where_conditions)
        
        # 总股票数
        cursor.execute(f"SELECT COUNT(*) FROM finviz_screener {where_clause}", params)
        total_stocks = cursor.fetchone()[0]
        
        # 行业分布
        cursor.execute(f"""
            SELECT sector, COUNT(*) as count 
            FROM finviz_screener {where_clause}
            GROUP BY sector 
            ORDER BY count DESC
        """, params)
        sector_dist = [{"sector": row[0], "count": row[1], "percentage": round(row[1]/total_stocks*100, 2)} 
                      for row in cursor.fetchall() if row[0]]
        
        # 国家分布
        cursor.execute(f"""
            SELECT country, COUNT(*) as count 
            FROM finviz_screener {where_clause}
            GROUP BY country 
            ORDER BY count DESC
        """, params)
        country_dist = [{"country": row[0], "count": row[1], "percentage": round(row[1]/total_stocks*100, 2)} 
                       for row in cursor.fetchall() if row[0]]
        
        # 市值分布
        cursor.execute(f"""
            SELECT 
                CASE 
                    WHEN market_cap < 50000000 THEN 'Nano (<$50M)'
                    WHEN market_cap < 300000000 THEN 'Micro ($50M-$300M)'
                    WHEN market_cap < 2000000000 THEN 'Small ($300M-$2B)'
                    WHEN market_cap < 10000000000 THEN 'Mid ($2B-$10B)'
                    WHEN market_cap < 100000000000 THEN 'Large ($10B-$100B)'
                    ELSE 'Mega ($100B+)'
                END as range,
                COUNT(*) as count
            FROM finviz_screener {where_clause} AND market_cap IS NOT NULL
            GROUP BY 1
            ORDER BY MIN(market_cap)
        """, params)
        market_cap_dist = [{"range": row[0], "count": row[1], "percentage": round(row[1]/total_stocks*100, 2)} 
                          for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return {
            "totalStocks": total_stocks,
            "sectorDistribution": sector_dist,
            "countryDistribution": country_dist,
            "marketCapDistribution": market_cap_dist
        }
        
    except Exception as e:
        logger.error(f"获取筛选器统计失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取筛选器统计失败: {str(e)}")


@router.get("/sectors")
async def get_sectors(current_user: TokenData = Depends(get_current_user)):
    """获取所有行业板块"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT sector 
            FROM finviz_screener 
            WHERE sector IS NOT NULL AND sector != ''
            ORDER BY sector
        """)
        
        sectors = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return sectors
        
    except Exception as e:
        logger.error(f"获取行业板块失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取行业板块失败: {str(e)}")


@router.get("/industries")
async def get_industries(
    sector: Optional[str] = Query(None, description="行业板块"),
    current_user: TokenData = Depends(get_current_user)
):
    """获取细分行业"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if sector:
            cursor.execute("""
                SELECT DISTINCT industry 
                FROM finviz_screener 
                WHERE sector = %s AND industry IS NOT NULL AND industry != ''
                ORDER BY industry
            """, (sector,))
        else:
            cursor.execute("""
                SELECT DISTINCT industry 
                FROM finviz_screener 
                WHERE industry IS NOT NULL AND industry != ''
                ORDER BY industry
            """)
        
        industries = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return industries
        
    except Exception as e:
        logger.error(f"获取细分行业失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取细分行业失败: {str(e)}")


@router.get("/countries")
async def get_countries(current_user: TokenData = Depends(get_current_user)):
    """获取所有国家"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT country 
            FROM finviz_screener 
            WHERE country IS NOT NULL AND country != ''
            ORDER BY country
        """)
        
        countries = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return countries
        
    except Exception as e:
        logger.error(f"获取国家列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取国家列表失败: {str(e)}")


@router.get("/exchanges")
async def get_exchanges(current_user: TokenData = Depends(get_current_user)):
    """获取所有交易所"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT exchange 
            FROM finviz_screener 
            WHERE exchange IS NOT NULL AND exchange != ''
            ORDER BY exchange
        """)
        
        exchanges = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return exchanges
        
    except Exception as e:
        logger.error(f"获取交易所列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取交易所列表失败: {str(e)}")


@router.get("/stocks/{ticker}")
async def get_stock_detail(
    ticker: str,
    current_user: TokenData = Depends(get_current_user)
):
    """获取单个股票的详细信息"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM finviz_screener 
            WHERE ticker = %s AND snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener WHERE ticker = %s)
        """, (ticker, ticker))
        
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"股票 {ticker} 未找到")
        
        # 获取列名
        column_names = [desc[0] for desc in cursor.description]
        
        # 转换为字典
        stock_data = dict(zip(column_names, row))
        
        cursor.close()
        conn.close()
        
        return stock_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取股票详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取股票详情失败: {str(e)}")


@router.get("/update-status")
async def get_update_status(current_user: TokenData = Depends(get_current_user)):
    """获取数据更新状态"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取最新快照日期和总股票数
        cursor.execute("""
            SELECT MAX(snapshot_date) as last_update, COUNT(*) as total_stocks
            FROM finviz_screener
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener)
        """)
        
        result = cursor.fetchone()
        last_update = result[0] if result[0] else None
        total_stocks = result[1] if result[1] else 0
        
        cursor.close()
        conn.close()
        
        return {
            "last_update": last_update.isoformat() if last_update else None,
            "total_stocks": total_stocks,
            "is_updating": False,  # TODO: 实现更新状态检查
            "update_progress": None
        }
        
    except Exception as e:
        logger.error(f"获取更新状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取更新状态失败: {str(e)}")


@router.post("/update")
async def update_data(current_user: TokenData = Depends(get_current_user)):
    """手动触发数据更新"""
    try:
        # TODO: 实现数据更新逻辑
        # 这里应该调用数据更新服务
        
        logger.info(f"用户 {current_user.username} 触发了手动数据更新")
        
        return {
            "status": "success",
            "message": "数据更新已启动",
            "updated_count": 0,  # TODO: 返回实际更新数量
            "last_update": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"触发数据更新失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"触发数据更新失败: {str(e)}")
