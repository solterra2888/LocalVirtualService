# -*- coding: utf-8 -*-
"""
LLM Provider 官方计费单价（人民币/百万 tokens）

来源：
- Qwen（通义千问）：https://help.aliyun.com/zh/model-studio/model-pricing
  文档截取时间：2026-05
- Doubao（豆包/火山方舟）：https://www.volcengine.com/docs/82379/1544106
  文档截取时间：2026-05
- GLM（智谱 AI）：https://docs.bigmodel.cn / https://bigmodel.cn/pricing
  文档截取时间：2026-05

重要说明：
1. 阶梯计费（Qwen 部分模型）：按单次请求的输入 token 数量确定单价，
   落在哪个区间该请求所有 token 均按该区间单价结算（非累进）。
2. 价格变动：若官方调价，请同步更新本文件并记录更新日期。
3. 折扣不计入：Batch API 半价、上下文缓存折扣、思维链差异等特殊场景
   按普通实时推理价格计算（保守估计）。
4. GLM 部分模型为统一单价（不分输入/输出），此时 input_price == output_price。
5. Doubao doubao-seed-1-6 系列价格：参考官方发布信息（¥0.8/M 起），
   如有差异请以火山方舟官网 https://www.volcengine.com/docs/82379/1544106 为准。
"""

from typing import Dict, List, NamedTuple, Optional, Tuple


class PriceTier(NamedTuple):
    """单个计费阶梯。
    threshold: 本阶梯对应的最大输入 tokens 上限；None 表示无上限（最后一档）。
    input_price_per_m:  输入 tokens 单价（元/百万 tokens）。
    output_price_per_m: 输出 tokens 单价（元/百万 tokens）。
    """
    threshold: Optional[int]   # 含，None = 无上限
    input_price_per_m: float
    output_price_per_m: float


def _flat(input_price: float, output_price: float) -> List[PriceTier]:
    """辅助：无阶梯模型，直接返回单档定价列表。"""
    return [PriceTier(threshold=None, input_price_per_m=input_price, output_price_per_m=output_price)]


# ──────────────────────────────────────────────────────────────────────────────
# 阿里云通义（Qwen）模型定价
# 部署范围：中国内地
# 计费单位：元/百万 tokens
# 阶梯说明：prompt_tokens 落在哪个区间决定本次请求所用单价
# ──────────────────────────────────────────────────────────────────────────────
_QWEN_PRICING: Dict[str, List[PriceTier]] = {
    # ── Max 系列 ────────────────────────────────────────────────────────────────
    # qwen3.6-max-preview：分两档阶梯
    "qwen3.6-max-preview": [
        PriceTier(threshold=128_000,  input_price_per_m=9.0,  output_price_per_m=54.0),
        PriceTier(threshold=None,     input_price_per_m=15.0, output_price_per_m=90.0),
    ],
    # qwen3-max / qwen3-max-2026-01-23：三档阶梯
    "qwen3-max": [
        PriceTier(threshold=32_000,   input_price_per_m=2.5,  output_price_per_m=10.0),
        PriceTier(threshold=128_000,  input_price_per_m=4.0,  output_price_per_m=16.0),
        PriceTier(threshold=None,     input_price_per_m=7.0,  output_price_per_m=28.0),
    ],
    "qwen3-max-2026-01-23": [
        PriceTier(threshold=32_000,   input_price_per_m=2.5,  output_price_per_m=10.0),
        PriceTier(threshold=128_000,  input_price_per_m=4.0,  output_price_per_m=16.0),
        PriceTier(threshold=None,     input_price_per_m=7.0,  output_price_per_m=28.0),
    ],
    "qwen3-max-2025-09-23": [
        PriceTier(threshold=32_000,   input_price_per_m=6.0,  output_price_per_m=24.0),
        PriceTier(threshold=128_000,  input_price_per_m=10.0, output_price_per_m=40.0),
        PriceTier(threshold=None,     input_price_per_m=15.0, output_price_per_m=60.0),
    ],
    # qwen-max 系列：无阶梯
    "qwen-max":           _flat(2.4, 9.6),
    "qwen-max-latest":    _flat(2.4, 9.6),
    "qwen-max-2025-01-25": _flat(2.4, 9.6),
    "qwen-max-2024-09-19": _flat(20.0, 60.0),   # 旧定价，明显较高
    "qwen-max-2024-04-28": _flat(40.0, 120.0),
    "qwen-max-longcontext": _flat(2.4, 9.6),     # 等同 qwen-max，无官方独立定价

    # ── Plus 系列 ────────────────────────────────────────────────────────────────
    # qwen3.6-plus / qwen3.6-plus-2026-04-02：两档阶梯
    "qwen3.6-plus": [
        PriceTier(threshold=256_000, input_price_per_m=2.0,  output_price_per_m=12.0),
        PriceTier(threshold=None,    input_price_per_m=8.0,  output_price_per_m=48.0),
    ],
    "qwen3.6-plus-2026-04-02": [
        PriceTier(threshold=256_000, input_price_per_m=2.0,  output_price_per_m=12.0),
        PriceTier(threshold=None,    input_price_per_m=8.0,  output_price_per_m=48.0),
    ],
    # qwen3.5-plus：三档阶梯
    "qwen3.5-plus": [
        PriceTier(threshold=128_000, input_price_per_m=0.8,  output_price_per_m=4.8),
        PriceTier(threshold=256_000, input_price_per_m=2.0,  output_price_per_m=12.0),
        PriceTier(threshold=None,    input_price_per_m=4.0,  output_price_per_m=24.0),
    ],
    # qwen-plus 系列：三档阶梯
    "qwen-plus": [
        PriceTier(threshold=128_000, input_price_per_m=0.8,  output_price_per_m=2.0),
        PriceTier(threshold=256_000, input_price_per_m=2.4,  output_price_per_m=20.0),
        PriceTier(threshold=None,    input_price_per_m=4.8,  output_price_per_m=48.0),
    ],
    "qwen-plus-latest":    [
        PriceTier(threshold=128_000, input_price_per_m=0.8,  output_price_per_m=2.0),
        PriceTier(threshold=256_000, input_price_per_m=2.4,  output_price_per_m=20.0),
        PriceTier(threshold=None,    input_price_per_m=4.8,  output_price_per_m=48.0),
    ],
    "qwen-plus-2025-12-01": [
        PriceTier(threshold=128_000, input_price_per_m=0.8,  output_price_per_m=2.0),
        PriceTier(threshold=256_000, input_price_per_m=2.4,  output_price_per_m=20.0),
        PriceTier(threshold=None,    input_price_per_m=4.8,  output_price_per_m=48.0),
    ],
    "qwen-plus-2025-09-11": [
        PriceTier(threshold=128_000, input_price_per_m=0.8,  output_price_per_m=2.0),
        PriceTier(threshold=256_000, input_price_per_m=2.4,  output_price_per_m=20.0),
        PriceTier(threshold=None,    input_price_per_m=4.8,  output_price_per_m=48.0),
    ],
    "qwen-plus-2025-07-28": [
        PriceTier(threshold=128_000, input_price_per_m=0.8,  output_price_per_m=2.0),
        PriceTier(threshold=256_000, input_price_per_m=2.4,  output_price_per_m=20.0),
        PriceTier(threshold=None,    input_price_per_m=4.8,  output_price_per_m=48.0),
    ],
    "qwen-plus-2025-01-25": _flat(0.8, 2.0),
    "qwen-plus-2024-12-20": _flat(0.8, 2.0),

    # ── Flash / Turbo 系列 ───────────────────────────────────────────────────────
    # qwen3.6-flash
    "qwen3.6-flash": [
        PriceTier(threshold=256_000, input_price_per_m=1.2,  output_price_per_m=7.2),
        PriceTier(threshold=None,    input_price_per_m=4.8,  output_price_per_m=28.8),
    ],
    "qwen3.6-flash-2026-04-16": [
        PriceTier(threshold=256_000, input_price_per_m=1.2,  output_price_per_m=7.2),
        PriceTier(threshold=None,    input_price_per_m=4.8,  output_price_per_m=28.8),
    ],
    # qwen3.5-flash
    "qwen3.5-flash": [
        PriceTier(threshold=128_000, input_price_per_m=0.2,  output_price_per_m=2.0),
        PriceTier(threshold=256_000, input_price_per_m=0.8,  output_price_per_m=8.0),
        PriceTier(threshold=None,    input_price_per_m=1.2,  output_price_per_m=12.0),
    ],
    # qwen-flash
    "qwen-flash": [
        PriceTier(threshold=128_000, input_price_per_m=0.15, output_price_per_m=1.5),
        PriceTier(threshold=256_000, input_price_per_m=0.6,  output_price_per_m=6.0),
        PriceTier(threshold=None,    input_price_per_m=1.2,  output_price_per_m=12.0),
    ],
    # qwen-turbo 系列：无阶梯
    "qwen-turbo":           _flat(0.3, 3.0),
    "qwen-turbo-latest":    _flat(0.3, 3.0),
    "qwen-turbo-2025-07-15": _flat(0.3, 3.0),
    "qwen-turbo-2025-04-28": _flat(0.3, 3.0),
    "qwen-turbo-2025-02-11": _flat(0.3, 3.0),
    "qwen-turbo-2024-11-01": _flat(0.3, 3.0),

    # ── Long ───────────────────────────────────────────────────────────────────
    "qwen-long":        _flat(0.5, 2.0),
    "qwen-long-latest": _flat(0.5, 2.0),

    # ── Embedding（仅输入，无输出概念，此处 input=output=统一单价）─────────────────
    "text-embedding-v4": _flat(0.5, 0.5),
    "text-embedding-v3": _flat(0.5, 0.5),
    "text-embedding-v2": _flat(0.7, 0.7),
    "text-embedding-v1": _flat(0.7, 0.7),
}

# ──────────────────────────────────────────────────────────────────────────────
# 火山方舟（Doubao / 豆包）模型定价
# 参考：https://www.volcengine.com/docs/82379/1544106
# 计费单位：元/百万 tokens
# ──────────────────────────────────────────────────────────────────────────────
_DOUBAO_PRICING: Dict[str, List[PriceTier]] = {
    # doubao-seed-1-6 系列：三档阶梯
    # 参考发布资讯"低至 0.8 元/百万 tokens"（输入 ≤32K），按官方比例推算各档
    "doubao-seed-1-6-250615": [
        PriceTier(threshold=32_000,  input_price_per_m=0.8,   output_price_per_m=8.0),
        PriceTier(threshold=200_000, input_price_per_m=1.2,   output_price_per_m=16.0),
        PriceTier(threshold=None,    input_price_per_m=2.4,   output_price_per_m=24.0),
    ],
    "doubao-seed-1-6": [
        PriceTier(threshold=32_000,  input_price_per_m=0.8,   output_price_per_m=8.0),
        PriceTier(threshold=200_000, input_price_per_m=1.2,   output_price_per_m=16.0),
        PriceTier(threshold=None,    input_price_per_m=2.4,   output_price_per_m=24.0),
    ],
    # doubao-seed-2.0 系列
    "doubao-seed-2.0-pro": [
        PriceTier(threshold=32_000,  input_price_per_m=3.2,  output_price_per_m=16.0),
        PriceTier(threshold=None,    input_price_per_m=3.2,  output_price_per_m=16.0),
    ],
    "doubao-seed-2.0-lite": [
        PriceTier(threshold=32_000,  input_price_per_m=0.6,  output_price_per_m=3.6),
        PriceTier(threshold=None,    input_price_per_m=0.6,  output_price_per_m=3.6),
    ],
    # doubao-pro 系列（老版本模型）
    "doubao-pro-32k":   _flat(5.0, 15.0),
    "doubao-pro-128k":  _flat(5.0, 15.0),
    "doubao-lite-32k":  _flat(1.0,  1.0),
}

# ──────────────────────────────────────────────────────────────────────────────
# 智谱 AI（GLM）模型定价
# 参考：https://docs.bigmodel.cn / https://bigmodel.cn/pricing
# 文档截取时间：2026-06
# 计费单位：元/百万 tokens
#
# 重要说明：
# 1. GLM-5.1/5-Turbo/5 采用输入 token 阶梯计费（32K 分界）
# 2. GLM-4.7/4.5-Air 采用"输入+输出双重阶梯"，输出价格按保守值估算
#    （使用输出 >0.2M 的价格，避免低估成本）
# 3. GLM-4.7-FlashX 为固定价格（200K 上下文）
# ──────────────────────────────────────────────────────────────────────────────
_GLM_PRICING: Dict[str, List[PriceTier]] = {
    # ── GLM-5 系列（2026-06 新增）───────────────────────────────────────────────
    # GLM-5.1：最新旗舰，两档阶梯
    "glm-5.1": [
        PriceTier(threshold=32_000,  input_price_per_m=6.0,   output_price_per_m=24.0),
        PriceTier(threshold=None,    input_price_per_m=8.0,   output_price_per_m=28.0),
    ],
    # GLM-5-Turbo：高效模型，两档阶梯
    "glm-5-turbo": [
        PriceTier(threshold=32_000,  input_price_per_m=5.0,   output_price_per_m=22.0),
        PriceTier(threshold=None,    input_price_per_m=7.0,   output_price_per_m=26.0),
    ],
    # GLM-5：标准版，两档阶梯
    "glm-5": [
        PriceTier(threshold=32_000,  input_price_per_m=4.0,   output_price_per_m=18.0),
        PriceTier(threshold=None,    input_price_per_m=6.0,   output_price_per_m=22.0),
    ],

    # ── GLM-4.7 系列（2026-06 新增）───────────────────────────────────────────────
    # GLM-4.7：双重阶梯计费，输出价格保守估计（使用 >0.2M 的价格）
    # 实际计费还区分输出长度 ≤0.2M 和 >0.2M，此处取保守值避免低估
    "glm-4.7": [
        PriceTier(threshold=32_000,  input_price_per_m=3.0,   output_price_per_m=14.0),  # 输出 >0.2M 的价格
        PriceTier(threshold=200_000, input_price_per_m=4.0,   output_price_per_m=16.0),  # 长上下文
        PriceTier(threshold=None,    input_price_per_m=4.0,   output_price_per_m=16.0),
    ],
    # GLM-4.7-FlashX：高速推理，固定价格
    "glm-4.7-flashx": _flat(0.5, 3.0),
    # GLM-4.7-Flash：（表格截断，暂用 FlashX 价格估算）
    "glm-4.7-flash":  _flat(0.5, 3.0),

    # ── GLM-4.5 系列 ─────────────────────────────────────────────────────────────
    # GLM-4.5-Air：轻量级，双重阶梯，输出价格保守估计
    "glm-4.5-air": [
        PriceTier(threshold=32_000,  input_price_per_m=0.8,   output_price_per_m=6.0),   # 输出 >0.2M 的价格
        PriceTier(threshold=128_000, input_price_per_m=1.2,   output_price_per_m=8.0),
        PriceTier(threshold=None,    input_price_per_m=1.2,   output_price_per_m=8.0),
    ],

    # ── GLM-4.6 系列 ─────────────────────────────────────────────────────────────
    "glm-4.6":          _flat(4.0, 12.0),

    # ── GLM-4 系列（官方为统一单价）───────────────────────────────────────────────
    "glm-4-plus":       _flat(5.0,  5.0),
    "glm-4":            _flat(5.0,  5.0),   # 映射到 GLM-4-Plus 层级
    "glm-4-air":        _flat(0.5,  0.5),
    "glm-4-air-250414": _flat(0.5,  0.5),
    "glm-4-airx":       _flat(10.0, 10.0),
    "glm-4-flashx":         _flat(0.1, 0.1),
    "glm-4-flashx-250414":  _flat(0.1, 0.1),

    # glm-4-flash：免费
    "glm-4-flash":          _flat(0.0, 0.0),
    "glm-4-flash-250414":   _flat(0.0, 0.0),

    # glm-4-long：支持超长上下文
    "glm-4-long":       _flat(0.5, 0.5),
}

# ──────────────────────────────────────────────────────────────────────────────
# Anthropic Claude 模型定价（via CherryIN 网关）
# 参考：CherryIN 官方定价表（2026-06）
# 计费单位：元/百万 tokens（RMB）
# ──────────────────────────────────────────────────────────────────────────────
_CLAUDE_PRICING: Dict[str, List[PriceTier]] = {
    # ── CherryIN 网关模型名（前缀 anthropic/）────────────────────────────────
    "anthropic/claude-opus-4.7":            _flat(60.0, 240.0),
    "anthropic/claude-opus-4.5":            _flat(60.0, 240.0),
    "anthropic/claude-opus-4":              _flat(60.0, 240.0),
    "anthropic/claude-sonnet-4.6":          _flat(15.0,  60.0),
    "anthropic/claude-sonnet-4.5":          _flat(15.0,  60.0),
    "anthropic/claude-sonnet-4":            _flat(15.0,  60.0),
    "anthropic/claude-opus-4-20250514":     _flat(60.0, 240.0),
    "anthropic/claude-sonnet-4-20250514":   _flat(15.0,  60.0),
    "anthropic/claude-3-5-sonnet-20241022": _flat(15.0,  60.0),
    "anthropic/claude-3-5-haiku-20241022":  _flat(1.0,    4.0),
    "anthropic/claude-3-haiku-20240307":    _flat(1.0,    4.0),

    # ── Claude 4 系列（直接 API 名称） ────────────────────────────────────────
    "claude-opus-4.7":            _flat(60.0, 240.0),
    "claude-opus-4.5":            _flat(60.0, 240.0),
    "claude-opus-4-20250514":     _flat(60.0, 240.0),
    "claude-opus-4":              _flat(60.0, 240.0),
    "claude-sonnet-4.6":          _flat(15.0,  60.0),
    "claude-sonnet-4.5":          _flat(15.0,  60.0),
    "claude-sonnet-4-20250514":   _flat(15.0,  60.0),
    "claude-sonnet-4":            _flat(15.0,  60.0),

    # ── Claude 3.5 系列 ────────────────────────────────────────────────────────
    "claude-3-5-sonnet-20241022": _flat(15.0,  60.0),
    "claude-3-5-sonnet-20240620": _flat(15.0,  60.0),
    "claude-3-5-haiku-20241022":  _flat(1.0,    4.0),

    # ── Claude 3 系列 ─────────────────────────────────────────────────────────
    "claude-3-opus-20240229":     _flat(60.0, 240.0),
    "claude-3-sonnet-20240229":   _flat(15.0,  60.0),
    "claude-3-haiku-20240307":    _flat(1.0,    4.0),
}

# ──────────────────────────────────────────────────────────────────────────────
# DeepSeek AI 模型定价
# 参考：DeepSeek 官方定价（2026-06）
# 原价：美元/百万 tokens（USD）
#       deepseek-chat: $0.27 输入, $1.10 输出
#       deepseek-reasoner: $0.55 输入, $2.19 输出
# 计费单位：元/百万 tokens（RMB，汇率 ~7.2）
# ──────────────────────────────────────────────────────────────────────────────
_DEEPSEEK_PRICING: Dict[str, List[PriceTier]] = {
    # DeepSeek Chat (V3) — 通用对话模型
    "deepseek-chat": _flat(1.94, 7.92),   # $0.27/M → ¥1.94/M, $1.10/M → ¥7.92/M
    # DeepSeek Reasoner (R1) — 推理模型
    "deepseek-reasoner": _flat(3.96, 15.77),  # $0.55/M → ¥3.96/M, $2.19/M → ¥15.77/M

    # 用户自定义模型名（映射到 deepseek-chat）
    "deepseek-v4-pro": _flat(1.94, 7.92),
    "deepseek-v4-flash": _flat(1.94, 7.92),  # 暂用相同价格，如 flash 有更低价格请更新
}

# ──────────────────────────────────────────────────────────────────────────────
# Google Gemini 模型定价（via CherryIN 网关）
# 参考：CherryIN 官方定价表（2026-06）
# 计费单位：元/百万 tokens（RMB）
# ──────────────────────────────────────────────────────────────────────────────
_GEMINI_PRICING: Dict[str, List[PriceTier]] = {
    # Gemini 3.1 Pro Preview（对应 Gemini 2.5 Pro Preview）
    "gemini-3.1-pro-preview": _flat(63.0, 252.0),
    "google/gemini-3.1-pro-preview": _flat(63.0, 252.0),
    # Gemini 3.5 Flash（对应 Gemini 2.5 Flash）
    "gemini-3.5-flash": _flat(3.6, 14.4),
    "google/gemini-3.5-flash": _flat(3.6, 14.4),
    # Gemini 3.1 Flash Lite Preview（对应 Gemini 1.5 Flash）
    "gemini-3.1-flash-lite-preview": _flat(0.72, 2.88),
    "google/gemini-3.1-flash-lite-preview": _flat(0.72, 2.88),
    # 其他 Gemini 模型别名
    "gemini-2.5-pro-preview": _flat(63.0, 252.0),
    "gemini-2.5-flash": _flat(3.6, 14.4),
    "gemini-2.5-pro": _flat(63.0, 252.0),
    "gemini-2.0-flash": _flat(3.6, 14.4),
    "gemini-1.5-pro": _flat(18.0, 72.0),
    "gemini-1.5-flash": _flat(0.72, 2.88),
    # 通用别名
    "gemini-pro": _flat(63.0, 252.0),
    "gemini-flash": _flat(3.6, 14.4),
}

# 价格表最后更新时间（格式：YYYY-MM）
PRICING_LAST_UPDATED = {
    "qwen":    "2026-05",
    "doubao":  "2026-05",
    "glm":     "2026-06",  # 新增 GLM-5.1/GLM-4.7 系列，定价待官方确认
    "claude":  "2026-06",  # CherryIN 定价表
    "deepseek": "2026-06",
    "gemini":  "2026-06",  # CherryIN 定价表
}

_PROVIDER_TABLES: Dict[str, Dict[str, List[PriceTier]]] = {
    "qwen":   _QWEN_PRICING,
    "doubao": _DOUBAO_PRICING,
    "glm":    _GLM_PRICING,
    "claude": _CLAUDE_PRICING,
    "deepseek": _DEEPSEEK_PRICING,
    "gemini": _GEMINI_PRICING,
}


# ──────────────────────────────────────────────────────────────────────────────
# 公共接口
# ──────────────────────────────────────────────────────────────────────────────

def _lookup_tiers(provider: str, model: str) -> Optional[List[PriceTier]]:
    """从价格表中按 provider + model 精确或前缀匹配定价阶梯列表。"""
    table = _PROVIDER_TABLES.get(provider.lower())
    if table is None:
        return None

    model_lower = model.lower().strip()

    # 优先精确匹配
    if model_lower in table:
        return table[model_lower]

    # 前缀匹配（处理带日期后缀的模型名，例如 qwen-max-2024-09-19 → qwen-max）
    best_key: Optional[str] = None
    for key in table:
        if model_lower.startswith(key) or key.startswith(model_lower):
            if best_key is None or len(key) > len(best_key):
                best_key = key
    if best_key is not None:
        return table[best_key]

    return None


def _select_tier(tiers: List[PriceTier], prompt_tokens: int) -> PriceTier:
    """根据本次请求的输入 tokens 数量选择对应的计费阶梯。"""
    for tier in tiers:
        if tier.threshold is None or prompt_tokens <= tier.threshold:
            return tier
    return tiers[-1]


def calculate_cost(
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> Tuple[float, str]:
    """根据 provider、model 及本次调用 token 消耗计算应用层估算费用。

    Args:
        provider:           Provider 标识，如 "qwen" / "doubao" / "glm"。
        model:              实际调用的模型名，如 "qwen-max" / "doubao-seed-1-6-250615"。
        prompt_tokens:      本次请求输入（含 system prompt + 历史）的 token 数。
        completion_tokens:  本次请求输出的 token 数。

    Returns:
        (cost_rmb, currency)：估算费用（人民币元）和货币符号 "RMB"。

    注意：
        - 返回值为应用层估算，不含折扣、免费额度、Batch 半价等。
        - 若模型不在价格表中，降级为旧版固定系数（total_tokens × 0.01/M）并记录警告。
    """
    tiers = _lookup_tiers(provider, model)
    currency = "USD" if provider.lower() == "claude" else "RMB"

    if tiers is None:
        # 未知模型：保守估算（每千 tokens 0.01 RMB 或 0.01 USD）
        total_tokens = prompt_tokens + completion_tokens
        return total_tokens * 0.00001, currency

    tier = _select_tier(tiers, prompt_tokens)
    cost = (
        prompt_tokens     * tier.input_price_per_m  / 1_000_000
        + completion_tokens * tier.output_price_per_m / 1_000_000
    )
    return cost, currency


def get_model_pricing_info(provider: str, model: str) -> Optional[List[PriceTier]]:
    """返回指定模型的定价阶梯列表，供外部查询或测试。"""
    return _lookup_tiers(provider, model)
