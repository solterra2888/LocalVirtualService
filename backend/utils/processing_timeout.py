# -*- coding: utf-8 -*-
"""文件处理相关外部服务（ASR / LlamaParse / LLM）单阶段等待上限，由环境变量统一配置。"""

import os

_DEFAULT_MINUTES = 10.0


def get_file_processing_timeout_seconds() -> int:
    """
    单阶段最长等待时间（秒），来自 FILE_PROCESSING_TIMEOUT_MINUTES。
    用于：Fun-ASR 轮询、LlamaParse 任务轮询、LLM HTTP 读超时上限等。
    """
    raw = os.getenv("FILE_PROCESSING_TIMEOUT_MINUTES", str(int(_DEFAULT_MINUTES)))
    try:
        minutes = float(raw.strip())
    except ValueError:
        minutes = _DEFAULT_MINUTES
    if minutes <= 0:
        minutes = _DEFAULT_MINUTES
    return int(minutes * 60)
