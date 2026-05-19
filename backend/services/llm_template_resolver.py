# -*- coding: utf-8 -*-
"""
LLMTemplateResolver — 统一 LLM 模板解析层 (Phase 2 双态模型)

设计目标:
    所有 LLM 业务调用前都走这一层确定最终模板, 实现"一处修改, 全局生效"的
    模板默认值/私有覆盖/运行时覆盖机制.

PR-A/B 之后的优先级 (低 → 高):
    1. SYSTEM_DEFAULTS[module_code]            (兜底, 用于 SYSTEM_FIXED 模块)
    2. user-owned template (created_by=uid)    (USER_OWNED 模块的私有模板, 通常等于
                                                 用户在 My Templates 编辑后的当前内容)
    3. runtime_template_id                     (调用方运行时显式传入, 最高优先级)

旧"user_template_preferences" 路径已废弃:
    在新模型下, 每个 USER_OWNED 模块每用户唯一对应 1 行 user-scope 模板,
    用户通过编辑这一行来表达偏好, 不再用 preferences 中转表. 旧表仍保留用于
    向后兼容查询, 但 resolver 不读取.

模块路由:
    - SYSTEM_FIXED 模块 (in SYSTEM_ONLY_MODULES):
          → 直接走 SYSTEM_DEFAULTS, 跳过用户私有模板查询
    - USER_OWNED 模块 (in USER_OWNED_MODULES):
          → 优先返回该用户的 user-scope 模板; 缺失时懒触发
            UserTemplateSeederService 创建后再返回; 仍失败回退 SYSTEM_DEFAULTS

设计原则:
    - 不重复造轮子: 内部委托 TemplatePreferenceService 做模块判定,
                    委托 UserTemplateSeederService 做懒初始化
    - 统一异常: ResolveError
    - 失效降级: runtime → user-owned → system default
    - 可观测: 所有解析都带 source 字段, 便于日志/审计追踪

使用示例:
    >>> resolver = LLMTemplateResolver()
    >>> # SYSTEM_FIXED 模块
    >>> resolved = resolver.resolve(user_id=42, module_code='news_summary')
    >>> resolved.source
    'default'

    >>> # USER_OWNED 模块, 用户已有私有模板
    >>> resolved = resolver.resolve(user_id=42, module_code='file_tagging')
    >>> resolved.source
    'user_owned'

    >>> # 运行时指定模板
    >>> resolved = resolver.resolve(user_id=42, module_code='file_analysis',
    ...                              runtime_template_id=99)
    >>> resolved.source
    'runtime'
"""
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, Optional

from backend.services.llm_prompt_template_service import LLMPromptTemplateService
from backend.services.template_preference_service import TemplatePreferenceService
from backend.services.user_template_seeder_service import UserTemplateSeederService
from backend.utils.logger import get_logger

logger = get_logger("llm_template_resolver")

# PR-B (2026-04-27): 引入 'user_owned' 替代 'preference', 反映新模型语义.
# 'preference' 仍保留以便向后兼容旧日志/数据分析, 实际新代码路径不会再产生它.
ResolveSource = Literal["runtime", "user_owned", "preference", "default"]


class ResolveError(Exception):
    """模板解析失败的统一异常."""


@dataclass
class ResolvedTemplate:
    """LLMTemplateResolver.resolve() 的返回值.

    所有 LLM 调用方拿到这个对象后:
      - 用 `template_id` 传给 `LocalLLMAnalysisService(template_id=...)` 或
        `llm_service.chat_completion_with_template(template_id=..., ...)`
      - 用 `source` 写日志, 便于审计 "这次调用用的哪条路径选的模板"
      - 用 `recommended_config` 看模板自带的 provider/model/temperature 推荐
    """

    template_id: int
    template_code: str
    template_name: str
    module_code: str
    source: ResolveSource
    scope: str  # 'system' / 'user' / 'shared'
    recommended_config: Dict[str, Any] = field(default_factory=dict)
    # recommended_config 含: provider / model / temperature / max_tokens

    def __repr__(self) -> str:
        return (
            f"ResolvedTemplate(id={self.template_id}, code={self.template_code!r}, "
            f"module={self.module_code!r}, source={self.source!r}, scope={self.scope!r})"
        )


class LLMTemplateResolver:
    """统一 LLM 模板解析服务."""

    def __init__(
        self,
        preference_service: Optional[TemplatePreferenceService] = None,
        template_service: Optional[LLMPromptTemplateService] = None,
        seeder_service: Optional[UserTemplateSeederService] = None,
    ):
        """支持依赖注入便于单测."""
        self.pref = preference_service or TemplatePreferenceService()
        self.tpl = template_service or LLMPromptTemplateService()
        self.seeder = seeder_service or UserTemplateSeederService(
            preference_service=self.pref,
            template_service=self.tpl,
        )

    # ──────────────────────────────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────────────────────────────

    def resolve(
        self,
        module_code: str,
        user_id: Optional[int] = None,
        runtime_template_id: Optional[int] = None,
        prefer_system_default: bool = False,
    ) -> ResolvedTemplate:
        """解析最终使用的 LLM 模板.

        Args:
            module_code: 业务模块标识 (与 MODULE_REGISTRY 对齐)
            user_id: 当前用户 ID; 若为 None 则跳过偏好查询直走 default
            runtime_template_id: 调用方运行时显式指定的 template_id;
                例如上传弹窗用户当场选的模板. 命中即返回.
            prefer_system_default: 为 True 时跳过 user-owned 私有模板, 直接使用
                系统默认 (用于自动打标等需跟随 admin 系统模板更新的场景).

        Returns:
            ResolvedTemplate

        Raises:
            ResolveError: 三级 fallback 全部失败时抛出
        """
        if not module_code:
            raise ResolveError("module_code is required")

        # ── 1. runtime override (最高优先级) ──────────────────────────
        if runtime_template_id:
            tpl = self.tpl.get_template(runtime_template_id)
            if tpl and tpl.get("is_active"):
                logger.info(
                    "runtime template hit",
                    user_id=user_id,
                    module_code=module_code,
                    template_id=runtime_template_id,
                    template_code=tpl["template_code"],
                )
                return self._build_resolved(tpl, module_code, "runtime")
            # runtime 模板失效不抛错, 降级到 preference / default
            logger.warning(
                "runtime template invalid (not found / inactive), falling back",
                user_id=user_id,
                module_code=module_code,
                runtime_template_id=runtime_template_id,
            )

        # ── 2. user-owned template (USER_OWNED 模块且有 user_id) ─────────
        # SYSTEM_FIXED 模块跳过此步, 直接走系统默认.
        # PR-B (2026-04-27): user-scope 模板已 seed, 这里成为主路径.
        is_user_owned = module_code in self.pref.USER_OWNED_MODULES
        if user_id is not None and is_user_owned:
            owned_tpl = self._lookup_user_owned(user_id, module_code)
            if owned_tpl is None:
                # 懒初始化: 新用户或 PR-B migration 未覆盖的边缘情况.
                # 失败时不抛错, 静默降级到系统默认.
                try:
                    owned_tpl = self.seeder.ensure_user_template(user_id, module_code)
                except Exception as e:
                    logger.warning(
                        "lazy seed user template failed, falling back to system default",
                        user_id=user_id,
                        module_code=module_code,
                        error=str(e),
                    )

            if owned_tpl and owned_tpl.get("is_active"):
                logger.info(
                    "user-owned template hit",
                    user_id=user_id,
                    module_code=module_code,
                    template_id=owned_tpl["id"],
                    template_code=owned_tpl["template_code"],
                )
                return self._build_resolved(owned_tpl, module_code, "user_owned")

        # ── 3. system default ───────────────────────────────────────────
        default_code = self._lookup_default_code(module_code)
        if not default_code:
            raise ResolveError(
                f"module '{module_code}' has no system default and "
                f"no preference / runtime override could be used"
            )

        default_tpl = self.tpl.get_template_by_code(default_code)
        if not default_tpl or not default_tpl.get("is_active"):
            raise ResolveError(
                f"system default template '{default_code}' for module "
                f"'{module_code}' not found or inactive"
            )

        logger.info(
            "system default hit",
            user_id=user_id,
            module_code=module_code,
            template_code=default_code,
        )
        return self._build_resolved(default_tpl, module_code, "default")

    # ──────────────────────────────────────────────────────────────────
    # 便利接口
    # ──────────────────────────────────────────────────────────────────

    def resolve_code(
        self,
        module_code: str,
        user_id: Optional[int] = None,
        runtime_template_id: Optional[int] = None,
    ) -> str:
        """与 TemplatePreferenceService.resolve_template_code 对齐的便利接口.

        仅返回 template_code 字符串, 用于业务层只需要 code 的简化场景.
        """
        return self.resolve(module_code, user_id, runtime_template_id).template_code

    # ──────────────────────────────────────────────────────────────────
    # Internal
    # ──────────────────────────────────────────────────────────────────

    def _lookup_user_owned(
        self, user_id: int, module_code: str
    ) -> Optional[Dict[str, Any]]:
        """查 (user_id, module_code) 当前 active user-scope 模板的完整 dict.

        借用 seeder 的内部查询能力 (避免在 resolver 重复实现 SQL).
        返回 None 表示该用户在该模块下无私有模板, 调用方应触发懒 seed.
        """
        return self.seeder._find_user_template(user_id, module_code)

    def _lookup_default_code(self, module_code: str) -> Optional[str]:
        """按 SYSTEM_DEFAULTS → MODULE_REGISTRY.default_template_code 顺序查找."""
        # 优先 SYSTEM_DEFAULTS
        code = self.pref.SYSTEM_DEFAULTS.get(module_code)
        if code:
            return code

        # 回退 MODULE_REGISTRY
        for module in self.pref.MODULE_REGISTRY:
            if module["module_code"] == module_code:
                return module.get("default_template_code")
        return None

    def _build_resolved(
        self,
        tpl: Dict[str, Any],
        module_code: str,
        source: ResolveSource,
    ) -> ResolvedTemplate:
        """将 LLMPromptTemplateService.get_template 返回的 dict 包装为 ResolvedTemplate."""
        recommended_config: Dict[str, Any] = {}
        if tpl.get("recommended_provider"):
            recommended_config["provider"] = tpl["recommended_provider"]
        if tpl.get("recommended_model"):
            recommended_config["model"] = tpl["recommended_model"]
        if tpl.get("temperature") is not None:
            recommended_config["temperature"] = tpl["temperature"]
        if tpl.get("max_tokens") is not None:
            recommended_config["max_tokens"] = tpl["max_tokens"]

        return ResolvedTemplate(
            template_id=tpl["id"],
            template_code=tpl["template_code"],
            template_name=tpl.get("template_name", ""),
            module_code=module_code,
            source=source,
            scope=tpl.get("scope", "system"),
            recommended_config=recommended_config,
        )
