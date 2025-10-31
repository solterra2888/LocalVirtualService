"""
Theme Normalization Service
主题标准化服务 - 负责主题名称的标准化和映射
"""
from typing import Optional, List, Dict, Tuple
import re
from difflib import SequenceMatcher

from backend.database.db_manager import DatabaseManager
from backend.schemas.theme_unified import ThemeNormalizationResult
from backend.utils.logger import logger


class ThemeNormalizationService:
    """主题标准化服务"""
    
    # 常见的主题同义词映射（硬编码常用的）
    COMMON_ALIASES = {
        'ai': 'artificial_intelligence',
        'machine learning': 'artificial_intelligence',
        'ml': 'artificial_intelligence',
        'deep learning': 'artificial_intelligence',
        '人工智能': 'artificial_intelligence',
        '机器学习': 'artificial_intelligence',
        
        'ev': 'electric_vehicles',
        'electric car': 'electric_vehicles',
        '电动车': 'electric_vehicles',
        '电动汽车': 'electric_vehicles',
        '新能源汽车': 'electric_vehicles',
        
        '5g': '5g_technology',
        '5g network': '5g_technology',
        '5g通信': '5g_technology',
        
        'crypto': 'blockchain',
        'cryptocurrency': 'blockchain',
        'bitcoin': 'blockchain',
        '区块链': 'blockchain',
        '加密货币': 'blockchain',
        
        'cloud': 'cloud_computing',
        '云计算': 'cloud_computing',
        '云服务': 'cloud_computing',
        
        'biotech': 'biotechnology',
        'pharma': 'biotechnology',
        '生物技术': 'biotechnology',
        '制药': 'biotechnology',
        
        'fintech': 'fintech',
        '金融科技': 'fintech',
        
        'e-commerce': 'e_commerce',
        'ecommerce': 'e_commerce',
        '电商': 'e_commerce',
        '电子商务': 'e_commerce',
    }
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self._cache: Dict[str, str] = {}  # 缓存标准化结果
    
    # ============================================================
    # 主要标准化方法
    # ============================================================
    
    async def normalize(
        self,
        raw_name: str,
        use_fuzzy_match: bool = True,
        fuzzy_threshold: float = 0.85
    ) -> ThemeNormalizationResult:
        """
        标准化主题名称
        
        Args:
            raw_name: 原始主题名称
            use_fuzzy_match: 是否使用模糊匹配
            fuzzy_threshold: 模糊匹配的相似度阈值
        
        Returns:
            标准化结果
        """
        if not raw_name:
            return ThemeNormalizationResult(
                original_name=raw_name,
                canonical_name=raw_name,
                matched=False,
                confidence=0.0
            )
        
        # 清理名称
        cleaned_name = self._clean_name(raw_name)
        
        # 检查缓存
        if cleaned_name.lower() in self._cache:
            canonical = self._cache[cleaned_name.lower()]
            return ThemeNormalizationResult(
                original_name=raw_name,
                canonical_name=canonical,
                matched=True,
                confidence=1.0
            )
        
        # 1. 精确匹配 canonical_name
        result = await self._exact_match_canonical(cleaned_name)
        if result:
            self._cache[cleaned_name.lower()] = result.canonical_name
            return result
        
        # 2. 匹配 aliases（数据库）
        result = await self._match_aliases_db(cleaned_name)
        if result:
            self._cache[cleaned_name.lower()] = result.canonical_name
            return result
        
        # 3. 硬编码的常见同义词
        result = self._match_common_aliases(cleaned_name)
        if result:
            self._cache[cleaned_name.lower()] = result.canonical_name
            return result
        
        # 4. 模糊匹配
        if use_fuzzy_match:
            result = await self._fuzzy_match(cleaned_name, fuzzy_threshold)
            if result:
                self._cache[cleaned_name.lower()] = result.canonical_name
                return result
        
        # 5. 未匹配，返回清理后的名称
        logger.debug(f"主题 '{raw_name}' 未匹配到主数据，使用清理后的名称")
        return ThemeNormalizationResult(
            original_name=raw_name,
            canonical_name=cleaned_name,
            matched=False,
            confidence=0.5
        )
    
    async def normalize_batch(
        self,
        raw_names: List[str]
    ) -> List[ThemeNormalizationResult]:
        """批量标准化"""
        results = []
        for name in raw_names:
            result = await self.normalize(name)
            results.append(result)
        return results
    
    # ============================================================
    # 匹配方法
    # ============================================================
    
    async def _exact_match_canonical(self, cleaned_name: str) -> Optional[ThemeNormalizationResult]:
        """精确匹配 canonical_name"""
        query = """
        SELECT 
            id,
            canonical_name,
            display_name
        FROM theme_master_data
        WHERE LOWER(canonical_name) = LOWER($1)
          AND is_active = true
        LIMIT 1
        """
        
        result = await self.db.fetch_one(query, cleaned_name)
        
        if result:
            return ThemeNormalizationResult(
                original_name=cleaned_name,
                canonical_name=result['canonical_name'],
                matched=True,
                theme_master_id=result['id'],
                confidence=1.0
            )
        
        return None
    
    async def _match_aliases_db(self, cleaned_name: str) -> Optional[ThemeNormalizationResult]:
        """匹配数据库中的 aliases"""
        query = """
        SELECT 
            id,
            canonical_name,
            display_name,
            aliases
        FROM theme_master_data
        WHERE is_active = true
          AND aliases IS NOT NULL
          AND jsonb_array_length(aliases) > 0
        """
        
        results = await self.db.fetch_all(query)
        
        cleaned_lower = cleaned_name.lower()
        
        for row in results:
            aliases = row['aliases']
            if isinstance(aliases, list):
                for alias in aliases:
                    if alias and alias.lower() == cleaned_lower:
                        return ThemeNormalizationResult(
                            original_name=cleaned_name,
                            canonical_name=row['canonical_name'],
                            matched=True,
                            theme_master_id=row['id'],
                            confidence=0.95
                        )
        
        return None
    
    def _match_common_aliases(self, cleaned_name: str) -> Optional[ThemeNormalizationResult]:
        """匹配硬编码的常见同义词"""
        cleaned_lower = cleaned_name.lower()
        
        if cleaned_lower in self.COMMON_ALIASES:
            canonical = self.COMMON_ALIASES[cleaned_lower]
            return ThemeNormalizationResult(
                original_name=cleaned_name,
                canonical_name=canonical,
                matched=True,
                confidence=0.9
            )
        
        return None
    
    async def _fuzzy_match(
        self,
        cleaned_name: str,
        threshold: float = 0.85
    ) -> Optional[ThemeNormalizationResult]:
        """
        模糊匹配
        使用相似度算法找到最接近的主题
        """
        query = """
        SELECT 
            id,
            canonical_name,
            display_name
        FROM theme_master_data
        WHERE is_active = true
        """
        
        results = await self.db.fetch_all(query)
        
        best_match = None
        best_similarity = 0.0
        
        cleaned_lower = cleaned_name.lower()
        
        for row in results:
            # 与 canonical_name 比较
            canonical_lower = row['canonical_name'].lower()
            similarity = self._calculate_similarity(cleaned_lower, canonical_lower)
            
            if similarity > best_similarity:
                best_similarity = similarity
                best_match = row
            
            # 与 display_name 比较
            if row['display_name']:
                display_lower = row['display_name'].lower()
                similarity = self._calculate_similarity(cleaned_lower, display_lower)
                
                if similarity > best_similarity:
                    best_similarity = similarity
                    best_match = row
        
        if best_match and best_similarity >= threshold:
            logger.debug(f"模糊匹配成功: '{cleaned_name}' -> '{best_match['canonical_name']}' (相似度: {best_similarity:.2f})")
            return ThemeNormalizationResult(
                original_name=cleaned_name,
                canonical_name=best_match['canonical_name'],
                matched=True,
                theme_master_id=best_match['id'],
                confidence=best_similarity
            )
        
        return None
    
    # ============================================================
    # 辅助方法
    # ============================================================
    
    def _clean_name(self, name: str) -> str:
        """
        清理主题名称
        - 去除特殊字符
        - 标准化空格
        - 去除前后空格
        """
        # 去除特殊字符（保留字母、数字、空格、下划线、连字符）
        name = re.sub(r'[^a-zA-Z0-9\s_\-\u4e00-\u9fff]', '', name)
        
        # 标准化空格
        name = re.sub(r'\s+', ' ', name)
        
        # 去除前后空格
        name = name.strip()
        
        # 如果包含中文，保持原样；否则转为小写
        if self._contains_chinese(name):
            return name
        else:
            # 将空格替换为下划线（用于canonical_name格式）
            # name = name.lower().replace(' ', '_')
            return name
    
    def _contains_chinese(self, text: str) -> bool:
        """检查文本是否包含中文"""
        return bool(re.search(r'[\u4e00-\u9fff]', text))
    
    def _calculate_similarity(self, s1: str, s2: str) -> float:
        """
        计算两个字符串的相似度（0-1）
        使用 SequenceMatcher
        """
        return SequenceMatcher(None, s1, s2).ratio()
    
    # ============================================================
    # 主数据管理
    # ============================================================
    
    async def add_alias(
        self,
        canonical_name: str,
        alias: str
    ) -> bool:
        """
        为主题添加同义词
        
        Args:
            canonical_name: 标准化名称
            alias: 要添加的同义词
        
        Returns:
            是否成功
        """
        # 先查询该主题
        query = """
        SELECT id, aliases
        FROM theme_master_data
        WHERE canonical_name = $1
          AND is_active = true
        """
        
        result = await self.db.fetch_one(query, canonical_name)
        
        if not result:
            logger.warning(f"主题 '{canonical_name}' 不存在")
            return False
        
        # 获取现有aliases
        aliases = result['aliases'] if result['aliases'] else []
        
        # 检查是否已存在
        if alias in aliases:
            logger.debug(f"同义词 '{alias}' 已存在")
            return True
        
        # 添加新同义词
        aliases.append(alias)
        
        # 更新数据库
        update_query = """
        UPDATE theme_master_data
        SET aliases = $1::jsonb,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $2
        """
        
        await self.db.execute(update_query, aliases, result['id'])
        
        logger.info(f"为主题 '{canonical_name}' 添加了同义词 '{alias}'")
        
        # 清除缓存
        self._cache.clear()
        
        return True
    
    async def suggest_merge_candidates(
        self,
        similarity_threshold: float = 0.85
    ) -> List[Tuple[str, str, float]]:
        """
        建议可能需要合并的主题
        返回相似度高的主题对
        
        Returns:
            [(theme1, theme2, similarity), ...]
        """
        query = """
        SELECT canonical_name, display_name
        FROM theme_master_data
        WHERE is_active = true
        ORDER BY canonical_name
        """
        
        results = await self.db.fetch_all(query)
        
        candidates = []
        
        # 两两比较
        for i in range(len(results)):
            for j in range(i + 1, len(results)):
                theme1 = results[i]['canonical_name']
                theme2 = results[j]['canonical_name']
                
                similarity = self._calculate_similarity(
                    theme1.lower(),
                    theme2.lower()
                )
                
                if similarity >= similarity_threshold:
                    candidates.append((theme1, theme2, similarity))
        
        # 按相似度降序排序
        candidates.sort(key=lambda x: x[2], reverse=True)
        
        return candidates
    
    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()
        logger.info("主题标准化缓存已清除")



