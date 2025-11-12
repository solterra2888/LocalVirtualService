# -*- coding: utf-8 -*-
"""
投资主题批量上传文件解析工具
支持 CSV 和 Excel 格式

核心逻辑：
1. theme 列必须只有一个唯一值（整个文件）
2. 按 (theme, category) 组合分组
3. 每个组合创建一个主题
例如：crypto.csv 中 theme='crypto'，8 个不同 category，创建 8 个主题
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
    
    def parse_file(self, file: UploadFile, market: str = 'US') -> Dict[str, Any]:
        """
        解析上传的文件（CSV 或 Excel）
        
        Args:
            file: FastAPI UploadFile 对象
            market: 市场（US/HK/CN），由用户在前端选择
        
        Returns:
            {
                'success': True/False,
                'theme_name': 'crypto',  # 主题名称（整个文件中唯一）
                'data': {
                    'crypto_ETF': {
                        'theme_name': 'crypto',
                        'category': 'ETF',
                        'tickers': ['ARKB', 'GBTC', 'ETHE', 'WT'],
                        'companies': ['ARK 21Shares Bitcoin ETF', ...],
                        'market': 'US'
                    },
                    'crypto_Treasury': {
                        'theme_name': 'crypto',
                        'category': 'Treasury',
                        'tickers': ['MSTR', 'MARA', ...],
                        'companies': [...],
                        'market': 'US'
                    },
                    ...
                },
                'preview': {
                    'total_themes': 8,  # 要创建的主题数量（category 数量）
                    'total_tickers': 30,
                    'categories': {'ETF': 4, 'Treasury': 9, ...},
                    'categories_sample': ['ETF', 'Treasury', 'Exchange_Custody', ...]
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
            
            # 验证 theme 列只有一个唯一值
            theme_validation = self._validate_single_theme(df)
            if not theme_validation['success']:
                return theme_validation
            
            theme_name = theme_validation['theme_name']
            
            # 数据清洗和分组（按 theme + category 组合）
            grouped_data = self._group_by_theme_category(df, theme_name, market)
            
            # 生成预览信息
            preview = self._generate_preview(grouped_data, theme_name)
            
            return {
                'success': True,
                'theme_name': theme_name,
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
        
        df.rename(columns=column_mapping, inplace=True)
        
        return {'success': True}
    
    def _validate_single_theme(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        验证 theme 列只有一个唯一值
        """
        # 去除空值和空格
        df['theme'] = df['theme'].astype(str).str.strip()
        
        # 过滤掉 nan 和空字符串
        valid_themes = df[df['theme'].notna() & (df['theme'] != '') & (df['theme'] != 'nan')]
        
        if len(valid_themes) == 0:
            return {
                'success': False,
                'error': 'No valid theme name found in the file. Theme column cannot be empty.'
            }
        
        # 获取唯一值
        unique_themes = valid_themes['theme'].unique()
        
        if len(unique_themes) > 1:
            return {
                'success': False,
                'error': f'Multiple theme names found: {", ".join(unique_themes)}. The theme column must have only one unique value throughout the file.'
            }
        
        # 🔥 标准化主题名称：将下划线转换为空格，并转换为小写
        # 例如：'GS_Power_Up' -> 'gs power up'
        theme_name_raw = unique_themes[0]
        theme_name = self._normalize_text(theme_name_raw)
        
        logger.info(f"主题名称标准化: '{theme_name_raw}' -> '{theme_name}'")
        
        return {
            'success': True,
            'theme_name': theme_name
        }
    
    def _group_by_theme_category(self, df: pd.DataFrame, theme_name: str, market: str) -> Dict[str, Dict]:
        """
        按 (theme, category) 组合分组
        
        Args:
            df: DataFrame
            theme_name: 主题名称
            market: 市场（US/HK/CN）
        
        Returns:
            grouped_data: {
                'crypto_ETF': {
                    'theme_name': 'crypto',
                    'category': 'ETF',
                    'tickers': ['ARKB', 'GBTC', 'ETHE', 'WT'],
                    'companies': ['ARK 21Shares Bitcoin ETF', ...],
                    'market': 'US'
                },
                ...
            }
        """
        grouped_data = {}
        
        # 数据清洗：去除空值和空格
        df['category'] = df['category'].astype(str).str.strip()
        df['ticker'] = df['ticker'].astype(str).str.strip().str.upper()
        
        if 'company' in df.columns:
            df['company'] = df['company'].astype(str).str.strip()
        
        # 过滤掉无效行
        df = df[
            (df['category'].notna()) & 
            (df['category'] != '') & 
            (df['category'] != 'nan') &
            (df['ticker'].notna()) & 
            (df['ticker'] != '') & 
            (df['ticker'] != 'NAN')
        ]
        
        # 按 category 分组
        grouped = df.groupby('category')
        
        for category_raw, group in grouped:
            # 🔥 标准化分类名称：将下划线转换为空格，并转换为小写
            # 例如：'Electric_Utility' -> 'electric utility'
            category = self._normalize_text(category_raw)
            
            logger.debug(f"分类标准化: '{category_raw}' -> '{category}'")
            
            # 生成主题 ID（使用标准化后的名称，但用下划线连接以保持ID格式）
            # 例如：'gs power up' + 'electric utility' -> 'gs_power_up_electric_utility'
            theme_id_part1 = theme_name.replace(' ', '_')
            theme_id_part2 = category.replace(' ', '_')
            theme_id = f"{theme_id_part1}_{theme_id_part2}"
            
            # 获取 tickers（去重并排序）
            tickers = sorted(list(set(group['ticker'].tolist())))
            
            # 获取 companies（如果有）
            companies = []
            if 'company' in group.columns:
                # 创建 ticker -> company 映射
                ticker_company_map = {}
                for _, row in group.iterrows():
                    ticker = row['ticker']
                    company = row.get('company', '')
                    if ticker and company and company != 'nan':
                        ticker_company_map[ticker] = company
                
                # 按 ticker 顺序生成 company 列表
                companies = [ticker_company_map.get(t, '') for t in tickers]
            
            grouped_data[theme_id] = {
                'theme_name': theme_name,
                'category': category,
                'tickers': tickers,
                'companies': companies,
                'market': market
            }
        
        return grouped_data
    
    def _generate_preview(self, grouped_data: Dict, theme_name: str) -> Dict[str, Any]:
        """
        生成预览信息
        
        Returns:
            {
                'total_themes': 8,  # 要创建的主题数量（= category 数量）
                'total_tickers': 30,
                'categories': {'ETF': 4, 'Treasury': 9, ...},  # category -> ticker count
                'categories_sample': ['ETF', 'Treasury', 'Exchange_Custody', ...]  # 前5个category
            }
        """
        total_themes = len(grouped_data)
        total_tickers = sum(len(data['tickers']) for data in grouped_data.values())
        
        # 统计每个 category 的 ticker 数量
        categories = {}
        for data in grouped_data.values():
            category = data['category']
            ticker_count = len(data['tickers'])
            categories[category] = ticker_count
        
        # 取前 5 个 category 作为示例
        categories_sample = list(categories.keys())[:5]
        
        return {
            'theme_name': theme_name,
            'total_themes': total_themes,
            'total_tickers': total_tickers,
            'categories': categories,
            'categories_sample': categories_sample
        }

