# -*- coding: utf-8 -*-
"""
UserTemplateSeederService — Phase 2 双态模型用户模板初始化与重置服务

新模板架构 (PR-A/PR-B 之后):
    - SYSTEM_FIXED 模块: 固定使用 system 模板, 不允许 per-user 自定义
    - USER_OWNED 模块  : 每用户在每个模块下有且仅有 1 行 user-scope 模板
                          (依赖 partial unique index uq_user_scope_module 保证)

本服务的职责:
    1. seed_user(user_id)              懒初始化, 给新注册用户生成完整 7 行
                                        USER_OWNED 模板 (从 system default clone)
    2. ensure_user_template(user_id, module_code)
                                        单模块懒初始化, 解析时按需调用
    3. reset_module(user_id, module_code)
                                        重置 = 用 system default 内容覆盖用户当前
                                        私有模板 (无历史, 不新增行)
    4. seed_all_users()                 后台批量补齐 (PR-B migration 已用 SQL 完成,
                                        本方法仅为运维兜底)

设计要点:
    - 复用 LLMPromptTemplateService 现有 CRUD, 不直接写 SQL
    - 命名约定: template_code = 'user_{user_id}_{module_code}'
    - parent_template_id 字段记录"克隆自哪个 system 模板", 便于 reset 时回溯
    - 全部操作幂等; 唯一约束兜底, 即使竞态也只剩 1 行

调用方:
    - PR-C: 用户登录 / My Templates 页加载时调 seed_user 兜底
    - PR-C: My Templates 页 "Reset to default" 按钮调 reset_module
    - LLMTemplateResolver: 解析时若用户私有模板缺失则调 ensure_user_template
"""
import json
from typing import Any, Dict, List, Optional
import json

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.services.template_preference_service import TemplatePreferenceService
from backend.services.llm_prompt_template_service import LLMPromptTemplateService

logger = get_logger("user_template_seeder_service")


class SeederError(Exception):
    """用户模板 seed/reset 失败时抛出."""


class UserTemplateSeederService:
    """每用户每 USER_OWNED module 1 行私有模板的初始化/重置服务."""

    # 复制 system → user-scope 时需透传的字段
    _CLONE_FIELDS = (
        "system_prompt",
        "user_prompt_template",
        "variables",
        "output_format",
        "recommended_provider",
        "recommended_model",
        "temperature",
        "max_tokens",
        "tags",
        "category_id",
    )

    def __init__(
        self,
        preference_service: Optional[TemplatePreferenceService] = None,
        template_service: Optional[LLMPromptTemplateService] = None,
    ) -> None:
        self.pref = preference_service or TemplatePreferenceService()
        self.tpl = template_service or LLMPromptTemplateService()
        self.db_config = get_database_config()
        self.db_manager = DatabaseManager(self.db_config)

    # ──────────────────────────────────────────────────────────────────
    # 公开 API
    # ──────────────────────────────────────────────────────────────────

    def seed_user(self, user_id: int) -> Dict[str, Any]:
        """为单个用户在所有 USER_OWNED 模块下补齐私有模板.

        Returns:
            { 'created': [...], 'skipped': [...], 'failed': [...] }
        """
        if not user_id:
            raise SeederError("user_id is required")

        result: Dict[str, List[str]] = {"created": [], "skipped": [], "failed": []}
        for module_code in sorted(self.pref.USER_OWNED_MODULES):
            try:
                created = self._ensure_one(user_id, module_code)
                if created:
                    result["created"].append(module_code)
                else:
                    result["skipped"].append(module_code)
            except Exception as e:
                logger.exception(
                    "seed_user failed",
                    user_id=user_id,
                    module_code=module_code,
                    error=str(e),
                )
                result["failed"].append(module_code)

        logger.info(
            "seed_user complete",
            user_id=user_id,
            created=len(result["created"]),
            skipped=len(result["skipped"]),
            failed=len(result["failed"]),
        )
        return result

    def ensure_user_template(self, user_id: int, module_code: str) -> Dict[str, Any]:
        """确保 (user_id, module_code) 存在 1 行 user-scope 模板; 不存在则懒创建.

        SYSTEM_FIXED 模块直接抛错——它们不应有 user-scope 行.
        Returns:
            user-scope 模板的完整 dict (与 LLMPromptTemplateService.get_template 同结构).
        """
        if module_code in self.pref.SYSTEM_ONLY_MODULES:
            raise SeederError(
                f"module '{module_code}' is SYSTEM_ONLY; user-owned templates not allowed"
            )
        if module_code not in self.pref.USER_OWNED_MODULES:
            raise SeederError(f"unknown USER_OWNED module: {module_code}")

        existing = self._find_user_template(user_id, module_code)
        if existing:
            return existing

        self._ensure_one(user_id, module_code)
        existing = self._find_user_template(user_id, module_code)
        if not existing:
            raise SeederError(
                f"failed to seed template for user {user_id} module {module_code}"
            )
        return existing

    def reset_module(self, user_id: int, module_code: str) -> Dict[str, Any]:
        """把用户的私有模板重置为 system default 内容 (覆盖, 无历史).

        实现策略:
            - 若 user-scope 行存在: UPDATE 该行的 prompt/variables/recommended_* 等字段
              指向 system default 的内容, 保留 id/template_code 不变
              (这样 user_template_preferences 等外键引用不会断)
            - 若 user-scope 行不存在: 等价于 ensure_user_template, 直接创建

        Returns:
            重置后该用户的私有模板 dict.
        """
        if module_code in self.pref.SYSTEM_ONLY_MODULES:
            raise SeederError(
                f"module '{module_code}' is SYSTEM_ONLY; reset not applicable"
            )
        sys_tpl = self._get_system_default(module_code)
        if not sys_tpl:
            raise SeederError(f"no system default template for module '{module_code}'")

        existing = self._find_user_template(user_id, module_code)
        if not existing:
            self._ensure_one(user_id, module_code)
            return self._find_user_template(user_id, module_code)  # type: ignore[return-value]

        # 直接 SQL 覆盖全部 clone 字段 + parent_template_id, 避免 update_template 白名单遗漏
        self._overwrite_user_template_from_system(existing["id"], sys_tpl)

        logger.info(
            "reset_module success",
            user_id=user_id,
            module_code=module_code,
            template_id=existing["id"],
            cloned_from=sys_tpl["id"],
        )
        return self._find_user_template(user_id, module_code)  # type: ignore[return-value]

    def seed_all_users(self) -> Dict[str, Any]:
        """运维兜底: 给当前所有用户补齐 USER_OWNED 模板. PR-B SQL 已完成, 仅作工具."""
        conn = self.db_manager.get_connection()
        if not conn:
            raise SeederError("DB connection failed")
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM users ORDER BY id")
            user_ids = [row[0] for row in cursor.fetchall()]
            cursor.close()
        finally:
            conn.close()

        summary: Dict[str, Any] = {"users_processed": 0, "total_created": 0}
        for uid in user_ids:
            res = self.seed_user(uid)
            summary["users_processed"] += 1
            summary["total_created"] += len(res["created"])
        return summary

    # ──────────────────────────────────────────────────────────────────
    # Internal
    # ──────────────────────────────────────────────────────────────────

    def _ensure_one(self, user_id: int, module_code: str) -> bool:
        """若 (user_id, module_code) 缺 user-scope 行则克隆 system default. 返回是否新建."""
        existing = self._find_user_template(user_id, module_code)
        if existing:
            return False

        sys_tpl = self._get_system_default(module_code)
        if not sys_tpl:
            raise SeederError(
                f"no system default template found for module '{module_code}', "
                f"cannot seed user template"
            )

        clone_data: Dict[str, Any] = {
            "template_code": f"user_{user_id}_{module_code}",
            "template_name": f"{sys_tpl.get('template_name', module_code)} (My)",
            "module_code": module_code,
            "scope": "user",
            "is_active": True,
            "is_default": False,
            "is_public": False,
            "description": (
                f"Cloned from system default {sys_tpl.get('template_code')}. "
                "Freely editable in My Templates."
            ),
        }
        for field in self._CLONE_FIELDS:
            if field in sys_tpl:
                clone_data[field] = sys_tpl[field]

        new_id = self.tpl.create_template(clone_data, user_id=user_id)
        if not new_id:
            raise SeederError(
                f"create_template returned None for user {user_id} module {module_code}"
            )

        # 写 parent_template_id (CRUD 服务白名单未包含, 直接 SQL 后置写)
        self._update_parent_template_id(new_id, sys_tpl["id"])

        logger.info(
            "user template seeded",
            user_id=user_id,
            module_code=module_code,
            template_id=new_id,
            cloned_from_template_id=sys_tpl["id"],
        )
        return True

    def _find_user_template(self, user_id: int, module_code: str) -> Optional[Dict[str, Any]]:
        """查 (user_id, module_code) 当前 active user-scope 模板."""
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id FROM llm_prompt_templates
                 WHERE created_by = %s AND module_code = %s
                   AND scope = 'user' AND is_active = true
                 LIMIT 1
                """,
                (user_id, module_code),
            )
            row = cursor.fetchone()
            cursor.close()
        finally:
            conn.close()

        if not row:
            return None
        return self.tpl.get_template(row[0])

    def _get_system_default(self, module_code: str) -> Optional[Dict[str, Any]]:
        """根据 SYSTEM_DEFAULTS 拿到该 module 的 system 默认完整模板 dict."""
        default_code = self.pref.SYSTEM_DEFAULTS.get(module_code)
        if not default_code:
            return None
        return self.tpl.get_template_by_code(default_code)

    def _overwrite_user_template_from_system(
        self, template_id: int, sys_tpl: Dict[str, Any]
    ) -> None:
        """用 system default 全量覆盖 user-scope 模板内容 (含 LLM 配置)."""
        conn = self.db_manager.get_connection()
        if not conn:
            raise SeederError("DB connection failed")
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE llm_prompt_templates SET
                    system_prompt = %s,
                    user_prompt_template = %s,
                    variables = %s,
                    output_format = %s,
                    recommended_provider = %s,
                    recommended_model = %s,
                    temperature = %s,
                    max_tokens = %s,
                    tags = %s,
                    category_id = %s,
                    parent_template_id = %s,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    sys_tpl.get("system_prompt"),
                    sys_tpl.get("user_prompt_template"),
                    json.dumps(sys_tpl.get("variables") or {}),
                    json.dumps(sys_tpl.get("output_format") or {}),
                    sys_tpl.get("recommended_provider"),
                    sys_tpl.get("recommended_model"),
                    sys_tpl.get("temperature"),
                    sys_tpl.get("max_tokens"),
                    json.dumps(sys_tpl.get("tags") or []),
                    sys_tpl.get("category_id"),
                    sys_tpl["id"],
                    template_id,
                ),
            )
            if cursor.rowcount != 1:
                raise SeederError(
                    f"overwrite user template failed for template_id={template_id}"
                )
            conn.commit()
            cursor.close()
        finally:
            conn.close()

    def _update_parent_template_id(self, template_id: int, parent_id: int) -> None:
        """直接写 parent_template_id 字段 (update_template 白名单未含)."""
        conn = self.db_manager.get_connection()
        if not conn:
            raise SeederError("DB connection failed")
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE llm_prompt_templates SET parent_template_id = %s, updated_at = NOW() WHERE id = %s",
                (parent_id, template_id),
            )
            conn.commit()
            cursor.close()
        finally:
            conn.close()
