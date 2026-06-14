# -*- coding: utf-8 -*-
"""UserTemplateSeederService.reset_module 行为测试."""

from unittest.mock import MagicMock, patch

import pytest

from backend.services.user_template_seeder_service import UserTemplateSeederService


@pytest.fixture
def seeder() -> UserTemplateSeederService:
    pref = MagicMock()
    pref.SYSTEM_ONLY_MODULES = frozenset()
    pref.USER_OWNED_MODULES = frozenset({"file_analysis"})
    pref.SYSTEM_DEFAULTS = {"file_analysis": "file_analysis_v1"}

    tpl_service = MagicMock()
    sys_tpl = {
        "id": 28,
        "template_code": "file_analysis_v1",
        "system_prompt": "system default",
        "user_prompt_template": "user default",
        "variables": {"file_content": {"type": "string", "required": True}},
        "output_format": {"type": "text"},
        "recommended_provider": "qwen",
        "recommended_model": "qwen3-max",
        "temperature": 0.5,
        "max_tokens": 8000,
        "tags": ["analysis"],
        "category_id": 3,
    }
    tpl_service.get_template_by_code.return_value = sys_tpl

    service = UserTemplateSeederService(
        preference_service=pref,
        template_service=tpl_service,
    )
    service._find_user_template = MagicMock(
        return_value={
            "id": 150,
            "recommended_provider": "claude",
            "recommended_model": "anthropic/claude-sonnet-4.6",
            "temperature": 1.0,
            "max_tokens": 20000,
        }
    )
    service._overwrite_user_template_from_system = MagicMock()
    service._find_user_template.side_effect = [
        {
            "id": 150,
            "recommended_provider": "claude",
            "recommended_model": "anthropic/claude-sonnet-4.6",
            "temperature": 1.0,
            "max_tokens": 20000,
        },
        {
            "id": 150,
            "recommended_provider": "qwen",
            "recommended_model": "qwen3-max",
            "temperature": 0.5,
            "max_tokens": 8000,
            "system_prompt": "system default",
            "user_prompt_template": "user default",
        },
    ]
    return service


def test_reset_module_overwrites_all_clone_fields_including_llm_config(seeder):
    result = seeder.reset_module(1, "file_analysis")

    seeder._overwrite_user_template_from_system.assert_called_once_with(150, seeder.tpl.get_template_by_code.return_value)
    assert result["recommended_provider"] == "qwen"
    assert result["recommended_model"] == "qwen3-max"
    assert result["temperature"] == 0.5
    assert result["max_tokens"] == 8000
