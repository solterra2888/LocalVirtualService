# -*- coding: utf-8 -*-
"""
用户模板偏好服务

管理用户针对各功能模块的 Prompt Template 偏好设置。
解析优先级：用户偏好 > 系统默认模板。
"""

from typing import Dict, List, Optional, Any

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("template_preference_service")


class TemplatePreferenceService:
    """用户模板偏好管理服务"""

    # 模块 → 系统默认模板 code 映射
    # PR-3 (2026-04-27): 扩展为统一 SYSTEM_DEFAULTS, 同时保留旧 MODULE_REGISTRY 的
    # default_template_code 字段; LLMTemplateResolver 优先读 SYSTEM_DEFAULTS,
    # 缺失则回退到 MODULE_REGISTRY[*].default_template_code.
    # PR-A (2026-04-27): 新增 audio_analysis / youtube_summary 模块 + feed_summary
    # 默认模板; 把 file_naming / file_classify / news_summary / feed_tagging 从
    # configurable 转 SYSTEM_ONLY (hybrid 模型 D4=B).
    SYSTEM_DEFAULTS: Dict[str, str] = {
        # —— Daily Reports ——
        'twitter_phase1': 'twitter_daily_phase1_default',
        'twitter_phase2': 'twitter_daily_phase2_default',
        'youtube_phase1': 'youtube_daily_phase1_default',
        'youtube_phase2': 'youtube_daily_phase2_default',
        # —— File 模块 ——
        'file_tagging':   'content_tag_v1',
        'file_naming':    'auto_name_v1',           # SYSTEM_ONLY (PR-A)
        'file_classify':  'auto_classify_v1',       # SYSTEM_ONLY (PR-A)
        'file_analysis':  'file_analysis_v1',       # PR-A: 单一通用模板 (孤儿 3 个已 archive)
        'audio_analysis': 'audio_analysis_v1',      # PR-A: 从 file_analysis 拆出
        'youtube_analysis': 'youtube_analysis_v1',  # 2026-05: Upload YouTube Link 专用深度分析
        # —— Feed 模块 ——
        'feed_tagging':   'content_tag_feeds_v1',   # SYSTEM_ONLY (PR-A, hybrid: 全局 tag)
        'feed_summary':   'feed_summary_v1',        # PR-A 种子: per-user Twitter feed 摘要
        'youtube_summary': 'youtube_summary_v1',    # PR-A 新增: per-video per-user 摘要
        # —— News 模块 ——
        'news_summary':       'news_summary_v1',     # SYSTEM_ONLY (PR-A)
        'news_classification': 'news_classification',
        # —— Theme 模块 ——
        'theme_description':  'theme_description_generator_v1',
    }

    # 固定使用系统模板的模块，不允许用户配置 (Phase 2 双态模型: SYSTEM_FIXED)
    # PR-A (2026-04-27): 在 D4=B hybrid 决策下扩大集合, 把 tag 类与系统级
    # 命名/归类/分类全部锁为系统模板, 仅 admin 可改 prompt 内容.
    SYSTEM_ONLY_MODULES = frozenset({
        'twitter_phase1',
        'youtube_phase1',
        'news_classification',  # 系统级新闻分类维度
        'news_summary',         # PR-A: 新闻摘要为全局共享
        'theme_description',    # 系统级主题描述生成
        'file_naming',          # PR-A: 文件自动命名为系统级
        'file_classify',        # PR-A: 文件自动归类为系统级
        'feed_tagging',         # PR-A: feed tag 全局共享 (hybrid 决策)
    })

    # 用户可拥有私有模板的模块 (Phase 2: USER_OWNED, 每用户每模块 1 行)
    # 派生属性: MODULE_REGISTRY 中 configurable=True 的模块 = USER_OWNED_MODULES
    USER_OWNED_MODULES = frozenset({
        'twitter_phase2',
        'youtube_phase2',
        'file_analysis',
        'audio_analysis',       # PR-A 新增
        'youtube_analysis',     # 2026-05: Upload YouTube Link 专用深度分析
        'file_tagging',
        'feed_summary',
        'youtube_summary',      # PR-A 新增
    })

    # 所有可注册模块的元信息（用于前端展示与 LLMTemplateResolver）
    MODULE_REGISTRY: List[Dict[str, Any]] = [
        # —— Daily Reports —————————————————————————————————————————————————
        {
            'module_code': 'twitter_phase1',
            'module_name': 'Twitter 作者摘要',
            'description': '按作者汇总当日推文（Phase 1，全局共享，不可定制）',
            'configurable': False,
            'default_template_code': 'twitter_daily_phase1_default',
        },
        {
            'module_code': 'twitter_phase2',
            'module_name': 'Twitter 日报',
            'description': '综合多位作者摘要生成日报（Phase 2，按用户）',
            'configurable': True,
            'default_template_code': 'twitter_daily_phase2_default',
        },
        {
            'module_code': 'youtube_phase1',
            'module_name': 'YouTube 频道摘要',
            'description': '按频道汇总当日视频（Phase 1，全局共享，不可定制）',
            'configurable': False,
            'default_template_code': 'youtube_daily_phase1_default',
        },
        {
            'module_code': 'youtube_phase2',
            'module_name': 'YouTube 日报',
            'description': '综合多个频道摘要生成日报（Phase 2，按用户）',
            'configurable': True,
            'default_template_code': 'youtube_daily_phase2_default',
        },
        # —— File 模块 ——————————————————————————————————————————————————————
        {
            'module_code': 'file_analysis',
            'module_name': '文件内容分析 (通用)',
            'description': '使用 LLM 模板深度分析上传的文档/网页/文本内容（per-user）',
            'configurable': True,
            'default_template_code': 'file_analysis_v1',
        },
        {
            # PR-A 新增: audio_analysis 从 file_analysis 模块拆出, 独立 per-user
            'module_code': 'audio_analysis',
            'module_name': '音频内容分析',
            'description': '对上传音频文件转写内容做结构化分析（per-user）',
            'configurable': True,
            'default_template_code': 'audio_analysis_v1',
        },
        {
            # 2026-05: Upload Sources YouTube Link 深度分析，独立于 Feeds youtube_summary
            'module_code': 'youtube_analysis',
            'module_name': 'YouTube 视频深度分析',
            'description': 'Upload Sources YouTube Link 的深度分析（per-user，独立于 Feeds 摘要模块）',
            'configurable': True,
            'default_template_code': 'youtube_analysis_v1',
        },
        {
            'module_code': 'file_tagging',
            'module_name': '文件自动打标',
            'description': '为上传的文件提取行业分类、ticker、概念等结构化标签（per-user）',
            'configurable': True,
            'default_template_code': 'content_tag_v1',
        },
        {
            # PR-A: 转 SYSTEM_ONLY, 仅 admin 可调
            'module_code': 'file_naming',
            'module_name': '文件自动命名',
            'description': '基于内容为文件生成简洁的展示名（系统级）',
            'configurable': False,
            'default_template_code': 'auto_name_v1',
        },
        {
            # PR-A: 转 SYSTEM_ONLY, 仅 admin 可调
            'module_code': 'file_classify',
            'module_name': '文件自动归类',
            'description': '把文件归入用户预定义的文件夹中最匹配的一个（系统级）',
            'configurable': False,
            'default_template_code': 'auto_classify_v1',
        },
        # —— Feed 模块 ——————————————————————————————————————————————————————
        {
            # PR-A: 转 SYSTEM_ONLY (hybrid: tag 全局共享, 仅 admin 可调)
            'module_code': 'feed_tagging',
            'module_name': 'Feed 内容打标',
            'description': '为 Twitter / YouTube feed 单条内容生成标签（全局共享，仅 admin 可调）',
            'configurable': False,
            'default_template_code': 'content_tag_feeds_v1',
        },
        {
            # PR-A: 已 seed feed_summary_v1 系统默认模板
            'module_code': 'feed_summary',
            'module_name': 'Feed 个性化摘要 (Twitter)',
            'description': '用户主动 Re-summarize Twitter feed 单条内容（per-user 视图）',
            'configurable': True,
            'default_template_code': 'feed_summary_v1',
        },
        {
            # PR-A 新增: per-video per-user 摘要 (区别于 feed_summary 偏 Twitter)
            'module_code': 'youtube_summary',
            'module_name': 'YouTube 视频个性化摘要',
            'description': '对单条 YouTube 视频生成 per-user 摘要（per-video，懒触发）',
            'configurable': True,
            'default_template_code': 'youtube_summary_v1',
        },
        # —— News 模块 ——————————————————————————————————————————————————————
        {
            # PR-A: 转 SYSTEM_ONLY, 全局共享, 仅 admin 可调
            'module_code': 'news_summary',
            'module_name': '新闻摘要',
            'description': '对单条新闻生成核心要点 + 市场影响摘要（系统级）',
            'configurable': False,
            'default_template_code': 'news_summary_v1',
        },
        {
            'module_code': 'news_classification',
            'module_name': '新闻分类维度',
            'description': '提取新闻的主题/事件/影响范围/紧急度等结构化分类（系统级）',
            'configurable': False,
            'default_template_code': 'news_classification',
        },
        # —— Theme 模块 ——————————————————————————————————————————————————————
        {
            'module_code': 'theme_description',
            'module_name': '主题描述生成',
            'description': '为投资主题生成描述文本（用于向量检索）（系统级）',
            'configurable': False,
            'default_template_code': 'theme_description_generator_v1',
        },
    ]

    def __init__(self):
        self.db_config = get_database_config()
        self.db_manager = DatabaseManager(self.db_config)

    # ── 模板解析 ──────────────────────────────────────────────────────────

    def resolve_template_code(self, user_id: int, module_code: str) -> str:
        """
        解析用户应使用的模板 template_code。

        - SYSTEM_ONLY 模块直接返回系统默认
        - 其他模块查询用户偏好，无偏好则 fallback 到系统默认

        Returns:
            最终使用的 template_code
        """
        if module_code in self.SYSTEM_ONLY_MODULES:
            default_code = self.SYSTEM_DEFAULTS.get(module_code)
            if not default_code:
                raise ValueError(f"SYSTEM_ONLY module '{module_code}' has no default template configured")
            return default_code

        pref = self._get_preference_row(user_id, module_code)
        if pref:
            template = self._validate_template(pref['template_id'])
            if template:
                logger.info(
                    f"用户偏好命中",
                    user_id=user_id,
                    module_code=module_code,
                    template_code=template['template_code'],
                )
                return template['template_code']
            else:
                logger.warning(
                    f"用户偏好指向的模板已失效，回退系统默认",
                    user_id=user_id,
                    module_code=module_code,
                    template_id=pref['template_id'],
                )

        default_code = self.SYSTEM_DEFAULTS.get(module_code)
        if default_code:
            return default_code

        raise ValueError(
            f"模块 '{module_code}' 无系统默认模板且用户未设置偏好"
        )

    # ── CRUD ──────────────────────────────────────────────────────────────

    def get_preference(self, user_id: int, module_code: str) -> Optional[Dict[str, Any]]:
        """获取用户在指定模块的偏好，包含模板详情。"""
        pref = self._get_preference_row(user_id, module_code)
        if not pref:
            return None

        template = self._validate_template(pref['template_id'])
        if not template:
            return None

        return {
            'id': pref['id'],
            'module_code': pref['module_code'],
            'template_id': pref['template_id'],
            'template_code': template['template_code'],
            'template_name': template['template_name'],
            'scope': template['scope'],
            'created_at': pref['created_at'],
            'updated_at': pref['updated_at'],
        }

    def list_user_preferences(self, user_id: int) -> List[Dict[str, Any]]:
        """列出用户的所有模板偏好。"""
        conn = self.db_manager.get_connection()
        if not conn:
            return []
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT p.id, p.module_code, p.template_id,
                       t.template_code, t.template_name, t.scope,
                       p.created_at, p.updated_at
                FROM user_template_preferences p
                JOIN llm_prompt_templates t ON p.template_id = t.id AND t.is_active = true
                WHERE p.user_id = %s
                ORDER BY p.module_code
            """, (user_id,))
            rows = cursor.fetchall()
            return [
                {
                    'id': r[0],
                    'module_code': r[1],
                    'template_id': r[2],
                    'template_code': r[3],
                    'template_name': r[4],
                    'scope': r[5],
                    'created_at': r[6].isoformat() if r[6] else None,
                    'updated_at': r[7].isoformat() if r[7] else None,
                }
                for r in rows
            ]
        except Exception as e:
            logger.error(f"Failed to list user preferences: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def set_preference(
        self,
        user_id: int,
        module_code: str,
        template_id: int,
        is_admin: bool = False,
    ) -> bool:
        """
        设置用户模板偏好，带权限校验。

        Raises:
            ValueError: 模块不可配或模板不存在
            PermissionError: 权限不足
        """
        if module_code in self.SYSTEM_ONLY_MODULES:
            raise ValueError(f"模块 '{module_code}' 固定使用系统模板，不支持自定义")

        template = self._validate_template(template_id)
        if not template:
            raise ValueError("模板不存在或已停用")

        if not is_admin:
            if template['created_by'] != user_id:
                raise PermissionError("只能选择自己创建的模板")
        else:
            if template['scope'] != 'system' and template.get('created_by') != user_id:
                raise PermissionError("只能选择系统模板或自己创建的模板")

        return self._upsert_preference(user_id, module_code, template_id)

    def delete_preference(self, user_id: int, module_code: str) -> bool:
        """删除偏好（恢复系统默认）。"""
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM user_template_preferences WHERE user_id = %s AND module_code = %s",
                (user_id, module_code),
            )
            deleted = cursor.rowcount > 0
            conn.commit()
            if deleted:
                logger.info(f"Deleted preference", user_id=user_id, module_code=module_code)
            return deleted
        except Exception as e:
            logger.error(f"Failed to delete preference: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()

    def get_modules(self) -> List[Dict[str, Any]]:
        """返回所有已注册模块的元信息。"""
        return [m.copy() for m in self.MODULE_REGISTRY]

    # ── Internal helpers ──────────────────────────────────────────────────

    def _get_preference_row(self, user_id: int, module_code: str) -> Optional[Dict[str, Any]]:
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, user_id, module_code, template_id, created_at, updated_at
                FROM user_template_preferences
                WHERE user_id = %s AND module_code = %s
            """, (user_id, module_code))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                'id': row[0],
                'user_id': row[1],
                'module_code': row[2],
                'template_id': row[3],
                'created_at': row[4],
                'updated_at': row[5],
            }
        except Exception as e:
            logger.error(f"Failed to get preference: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def _validate_template(self, template_id: int) -> Optional[Dict[str, Any]]:
        """验证模板存在且活跃。"""
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, template_code, template_name, scope, created_by, is_active
                FROM llm_prompt_templates
                WHERE id = %s AND is_active = true
            """, (template_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                'id': row[0],
                'template_code': row[1],
                'template_name': row[2],
                'scope': row[3],
                'created_by': row[4],
                'is_active': row[5],
            }
        except Exception as e:
            logger.error(f"Failed to validate template: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def _upsert_preference(self, user_id: int, module_code: str, template_id: int) -> bool:
        conn = self.db_manager.get_connection()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO user_template_preferences (user_id, module_code, template_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id, module_code)
                DO UPDATE SET template_id = EXCLUDED.template_id, updated_at = NOW()
            """, (user_id, module_code, template_id))
            conn.commit()
            logger.info(
                f"Upserted preference",
                user_id=user_id,
                module_code=module_code,
                template_id=template_id,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to upsert preference: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()
