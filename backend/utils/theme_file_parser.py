# -*- coding: utf-8 -*-
"""
投资主题批量上传文件解析工具
支持 CSV 和 Excel 格式

核心逻辑：
1. 支持文件中包含多个 theme 值
2. 按 (theme, category) 组合分组
3. 每个组合创建一个主题
例如：gs_themes.csv 中 theme='gs data driven' 与 theme='quantum computing'，
     各自按 category 分组后分别创建多条 investment theme 记录
"""

import pandas as pd
import io
from typing import Dict, List, Any
from fastapi import UploadFile
from backend.utils.logger import get_logger

logger = get_logger(__name__)


class ThemeFileParser:
    """主题文件解析器"""
    
    # 必填字段
    REQUIRED_COLUMNS = ['theme', 'category', 'ticker']
    
    # 可选字段
    OPTIONAL_COLUMNS = ['company']
    
    # 文件大小限制（10MB）
    MAX_FILE_SIZE = 10 * 1024 * 1024
    
    # 行数限制
    MAX_ROWS = 1000
    
    def __init__(self):
        """初始化解析器"""
        pass
    
    def _normalize_text(self, text: str) -> str:
        """
        标准化文本：将下划线转换为空格，并转换为小写
        
        规则：
        1. 将所有下划线 '_' 替换为空格 ' '
        2. 将所有字母转换为小写
        
        示例：
        - 'GS_Power_Up' -> 'gs power up'
        - 'Electric_Utility' -> 'electric utility'
        
        Args:
            text: 原始文本
            
        Returns:
            标准化后的文本
        """
        if not text:
            return text
        
        # 替换下划线为空格
        normalized = text.replace('_', ' ')
        
        # 转换为小写
        normalized = normalized.lower()
        
        return normalized
    
    def parse_file(self, file: UploadFile, market: str = 'US', group: str = None, creator_username: str = None) -> Dict[str, Any]:
        """
        解析上传的文件（CSV 或 Excel）
        
        Args:
            file: FastAPI UploadFile 对象
            market: 市场（US/HK/CN），由用户在前端选择
            group: 主题分组（如 AI、crypto 等），由用户在前端填写
            creator_username: 创建者用户名（用于生成唯一的theme_id）
        
        Returns:
            {
                'success': True/False,
                'theme_names': ['crypto', 'ai', ...],
                'data': {
                    'john_crypto_etf': {
                        'group': 'crypto',
                        'theme_name': 'crypto',
                        'category': 'etf',
                        'tickers': ['ARKB', 'GBTC', ...],
                        'companies': [...],
                        'market': 'US'
                    },
                    ...
                },
                'preview': {
                    'theme_names': [...],
                    'total_themes': 10,
                    'total_tickers': 30,
                    'themes': { 'crypto': { 'categories': {...}, 'total_tickers': 8 }, ... },
                    'categories': {...},
                    'categories_sample': [...]
                },
                'error': 'error message if failed'
            }
        """
        try:
            # 读取文件内容
            content = file.file.read()
            file_extension = self._get_file_extension(file.filename)
            
            # 解析为 DataFrame
            if file_extension == '.csv':
                df = self._parse_csv(content)
            elif file_extension in ['.xlsx', '.xls']:
                df = self._parse_excel(content)
            else:
                return {
                    'success': False,
                    'error': f'Unsupported file format: {file_extension}. Only .csv, .xlsx, .xls are supported.'
                }
            
            # 验证必需列
            validation_result = self._validate_columns(df)
            if not validation_result['success']:
                return validation_result
            
            # 验证 theme 列至少有一个有效值
            theme_validation = self._validate_themes(df)
            if not theme_validation['success']:
                return theme_validation
            
            # 数据清洗和分组（按 theme + category 组合）
            grouped_data = self._group_by_theme_category(df, market, group, creator_username)
            
            if not grouped_data:
                return {
                    'success': False,
                    'error': 'No valid rows found after filtering. Ensure theme, category, and ticker columns are filled.'
                }
            
            theme_names = sorted(set(data['theme_name'] for data in grouped_data.values()))
            
            # 生成预览信息
            preview = self._generate_preview(grouped_data, theme_names)
            
            return {
                'success': True,
                'theme_names': theme_names,
                'data': grouped_data,
                'preview': preview
            }
            
        except Exception as e:
            logger.error(f"Failed to parse file: {e}", exc_info=True)
            return {
                'success': False,
                'error': f'Failed to parse file: {str(e)}'
            }
    
    def _get_file_extension(self, filename: str) -> str:
        """获取文件扩展名"""
        if '.' not in filename:
            return ''
        return '.' + filename.rsplit('.', 1)[1].lower()
    
    def _parse_csv(self, content: bytes) -> pd.DataFrame:
        """解析 CSV 文件"""
        try:
            # 尝试 UTF-8
            df = pd.read_csv(io.BytesIO(content), encoding='utf-8')
            return df
        except UnicodeDecodeError:
            # 尝试 GBK
            try:
                df = pd.read_csv(io.BytesIO(content), encoding='gbk')
                return df
            except Exception as e:
                logger.warning(f"Failed to parse CSV with GBK encoding: {e}")
                # 最后尝试 ISO-8859-1
                df = pd.read_csv(io.BytesIO(content), encoding='iso-8859-1')
                return df
    
    def _parse_excel(self, content: bytes) -> pd.DataFrame:
        """解析 Excel 文件"""
        df = pd.read_excel(io.BytesIO(content))
        return df
    
    def _validate_columns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """验证必需列是否存在"""
        # 转换列名为小写进行比较
        df_columns_lower = [col.lower().strip() for col in df.columns]
        
        missing_columns = []
        for required_col in self.REQUIRED_COLUMNS:
            if required_col.lower() not in df_columns_lower:
                missing_columns.append(required_col)
        
        if missing_columns:
            return {
                'success': False,
                'error': f'Missing required columns: {", ".join(missing_columns)}. Required: theme, category, ticker.'
            }
        
        # 标准化列名（转换为小写）
        column_mapping = {}
        for col in df.columns:
            col_lower = col.lower().strip()
            if col_lower in [c.lower() for c in self.REQUIRED_COLUMNS + self.OPTIONAL_COLUMNS]:
                column_mapping[col] = col_lower
        
        # 使用copy方式避免SettingWithCopyWarning，但由于我们直接操作传入的df，这里用inplace=True没问题
        df.rename(columns=column_mapping, inplace=True)
        
        # 确保返回的字典不包含DataFrame引用
        return {'success': True}
    
    def _validate_themes(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        验证 theme 列至少有一个有效值（支持多个不同 theme）
        """
        df['theme'] = df['theme'].astype(str).str.strip()
        
        valid_themes = df[df['theme'].notna() & (df['theme'] != '') & (df['theme'] != 'nan')]
        
        if len(valid_themes) == 0:
            return {
                'success': False,
                'error': 'No valid theme name found in the file. Theme column cannot be empty.'
            }
        
        unique_themes = valid_themes['theme'].unique()
        logger.info(f"Found {len(unique_themes)} unique theme(s) in file: {', '.join(unique_themes[:5])}{'...' if len(unique_themes) > 5 else ''}")
        
        return {'success': True}
    
    def _group_by_theme_category(self, df: pd.DataFrame, market: str, group: str = None, creator_username: str = None) -> Dict[str, Dict]:
        """
        按 (theme, category) 组合分组
        
        Args:
            df: DataFrame
            market: 市场（US/HK/CN）
            group: 主题分组（如 AI、crypto 等）
            creator_username: 创建者用户名
        
        Returns:
            grouped_data: {
                'john_crypto_etf': {
                    'group': 'crypto',
                    'theme_name': 'crypto',
                    'category': 'etf',
                    'tickers': ['ARKB', 'GBTC', 'ETHE', 'WT'],
                    'companies': ['ARK 21Shares Bitcoin ETF', ...],
                    'market': 'US'
                },
                ...
            }
        """
        grouped_data = {}
        
        # 数据清洗：去除空值和空格
        df['theme'] = df['theme'].astype(str).str.strip()
        df['category'] = df['category'].astype(str).str.strip()
        df['ticker'] = df['ticker'].astype(str).str.strip().str.upper()
        
        if 'company' in df.columns:
            df['company'] = df['company'].astype(str).str.strip()
        
        # 过滤掉无效行
        df = df[
            (df['theme'].notna()) &
            (df['theme'] != '') &
            (df['theme'] != 'nan') &
            (df['category'].notna()) & 
            (df['category'] != '') & 
            (df['category'] != 'nan') &
            (df['ticker'].notna()) & 
            (df['ticker'] != '') & 
            (df['ticker'] != 'NAN')
        ]
        
        if df.empty:
            return grouped_data
        
        # 按 (theme, category) 分组
        grouped = df.groupby(['theme', 'category'])
        
        username_part = creator_username.replace(' ', '_').lower() if creator_username else 'unknown'
        
        for (theme_raw, category_raw), group_df in grouped:
            theme_name = self._normalize_text(theme_raw)
            category = self._normalize_text(category_raw)
            
            logger.debug(f"分组: theme '{theme_raw}' -> '{theme_name}', category '{category_raw}' -> '{category}'")
            
            # 生成主题 ID（格式：username_theme_name_category）
            theme_id_part1 = theme_name.replace(' ', '_')
            theme_id_part2 = category.replace(' ', '_')
            theme_id = f"{username_part}_{theme_id_part1}_{theme_id_part2}"
            
            # 获取 tickers（去重并排序）
            tickers = sorted(list(set(group_df['ticker'].tolist())))
            
            # 获取 companies（如果有）
            companies = []
            if 'company' in group_df.columns:
                ticker_company_map = {}
                for _, row in group_df.iterrows():
                    ticker = row['ticker']
                    company = row.get('company', '')
                    if ticker and company and company != 'nan':
                        ticker_company_map[ticker] = company
                
                companies = [ticker_company_map.get(t, '') for t in tickers]
            
            grouped_data[theme_id] = {
                'group': group,
                'theme_name': theme_name,
                'category': category,
                'tickers': tickers,
                'companies': companies,
                'market': market
            }
        
        return grouped_data
    
    def _generate_preview(self, grouped_data: Dict, theme_names: List[str]) -> Dict[str, Any]:
        """
        生成预览信息
        
        Returns:
            {
                'theme_names': [...],
                'total_themes': 10,
                'total_tickers': 30,
                'themes': {
                    'crypto': { 'categories': {'etf': 4}, 'total_tickers': 8 },
                    ...
                },
                'categories': {...},
                'categories_sample': [...]
            }
        """
        total_themes = len(grouped_data)
        total_tickers = sum(len(data['tickers']) for data in grouped_data.values())
        
        # 按 theme 分组统计
        themes_breakdown: Dict[str, Dict] = {}
        for name in theme_names:
            themes_breakdown[name] = {'categories': {}, 'total_tickers': 0}
        
        # 聚合所有 category 计数（legacy 兼容）
        categories: Dict[str, int] = {}
        
        for data in grouped_data.values():
            theme_name = data['theme_name']
            category = data['category']
            ticker_count = len(data['tickers'])
            
            if theme_name not in themes_breakdown:
                themes_breakdown[theme_name] = {'categories': {}, 'total_tickers': 0}
            
            themes_breakdown[theme_name]['categories'][category] = ticker_count
            themes_breakdown[theme_name]['total_tickers'] += ticker_count
            
            # legacy: 同名 category 跨 theme 时累加
            categories[category] = categories.get(category, 0) + ticker_count
        
        categories_sample = list(categories.keys())[:5]
        
        return {
            'theme_names': theme_names,
            'total_themes': total_themes,
            'total_tickers': total_tickers,
            'themes': themes_breakdown,
            'categories': categories,
            'categories_sample': categories_sample
        }
