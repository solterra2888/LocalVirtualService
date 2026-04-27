# -*- coding: utf-8 -*-
"""
本地LLM文件分析服务

⚠️ 重构说明 (PR-4, 2026-04-27):
    本服务 v1 版本是独立的 Path A 调用链(自己组装 PromptRenderer +
    ConfigResolver + chat_completion), 与 LLMService.chat_completion_with_template
    (Path B) 高度重叠却**不写 usage_log**, 导致系统级监控割裂.

    v2 版本将本服务降级为"语义包装层":
        - 对外 API 完全不变 (analyze_file_content / process_all_contents 签名不变)
        - 内部直接委托 llm_service.chat_completion_with_template
        - 自动写 llm_prompt_usage_logs (修复了原本路径 A 的可观测性缺口)
        - ConfigResolver / PromptRenderer / parse_output 全部由中央入口处理

    所有调用方 (ContentTaggingService / ContentNamingService / ContentClassifyService /
    FeedContentTaggingService / analyze_*_task 等) 零改动即可获得新能力.
"""

import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from backend.core.llm_framework import llm_service
from backend.utils.logger import get_logger

logger = get_logger("local_llm_analysis_service")


class LocalLLMAnalysisService:
    """本地 LLM 文件分析服务 (内部委托 chat_completion_with_template)"""

    def __init__(
        self,
        template_id: Optional[int] = None,
        template_code: Optional[str] = None,
        log_callback: Optional[Callable] = None,
    ):
        """
        初始化本地 LLM 分析服务.

        Args:
            template_id:   Prompt 模板 ID
            template_code: Prompt 模板代码 (template_id / template_code 至少其一)
            log_callback:  日志回调 (可选)
        """
        if not template_id and not template_code:
            raise ValueError("必须提供 template_id 或 template_code")

        self.template_id = template_id
        self.template_code = template_code
        self.log_callback = log_callback

        logger.info(
            "初始化本地 LLM 分析服务",
            template_id=template_id,
            template_code=template_code,
        )

    def log(self, message: str, level: str = "info"):
        """发送日志消息."""
        log_method = getattr(logger, level.lower(), logger.info)
        log_method(message)
        if self.log_callback:
            try:
                self.log_callback(log_message=message)
            except Exception:
                # log_callback 异常不应影响主流程
                pass

    # ──────────────────────────────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────────────────────────────

    async def analyze_file_content(
        self,
        file_content: str,
        file_name: Optional[str] = None,
        custom_variables: Optional[Dict[str, Any]] = None,
        user_id: Optional[int] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        分析单个文件内容.

        Args:
            file_content:     文件内容 (Markdown/纯文本)
            file_name:        文件名 (用于日志, 不传给 LLM)
            custom_variables: 自定义模板变量; 'file_content' 会被强制注入
            user_id:          用户 ID (传给 chat_completion_with_template, 进入 usage_log)
            provider:         指定 LLM 提供商 (覆盖模板推荐)
            model:            指定模型 (覆盖模板推荐)

        Returns:
            分析结果字典 (与 v1 版本保持完全兼容):
                report_content / total_tokens / total_price / response_time /
                model / provider / currency / model_usage
            额外字段:
                parsed_data:   chat_completion_with_template 解析模板 output_format 后的结构化数据
                template_id:   实际使用的模板 ID (从 chat_completion_with_template 的 response 反推)
        """
        start_time = time.time()
        file_prefix = f"[{file_name}] " if file_name else ""

        # 1. 准备模板变量 (强制注入 file_content, 覆盖 custom_variables 中的同名键)
        variables = dict(custom_variables) if custom_variables else {}
        variables["file_content"] = file_content

        self.log(
            f"{file_prefix}调用 chat_completion_with_template "
            f"(template_id={self.template_id} template_code={self.template_code} "
            f"variables={list(variables.keys())} user_id={user_id})"
        )

        try:
            # 2. 委托中央入口 (PromptRenderer + ConfigResolver + LLM + usage_log + parse_output)
            response = await llm_service.chat_completion_with_template(
                template_id=self.template_id,
                template_code=self.template_code,
                variables=variables,
                user_id=user_id,
                provider=provider,
                model=model,
            )
        except Exception as e:
            error_msg = f"{file_prefix}本地 LLM 分析失败: {e}"
            self.log(error_msg, "error")
            logger.error(error_msg, exc_info=True)
            raise

        total_time = time.time() - start_time
        tokens = response.usage.get("total_tokens", 0) if response.usage else 0

        self.log(
            f"{file_prefix}✅ LLM 分析完成: "
            f"总时间={total_time:.2f}s, "
            f"tokens={tokens}, "
            f"费用={response.cost:.6f} {response.currency}"
        )

        # 3. 适配旧版返回值结构 (保持外部 API 兼容)
        return {
            "report_content": response.content,
            "total_tokens": tokens,
            "total_price": response.cost,
            "response_time": total_time,
            "model": response.model,
            "provider": response.provider,
            "currency": response.currency,
            "model_usage": [
                {
                    "model_name": response.model,
                    "total_tokens": tokens,
                    "total_price": response.cost,
                    "currency": response.currency,
                }
            ],
            # 新增字段 (向前兼容: 旧调用方不读取也不报错)
            "parsed_data": getattr(response, "parsed_data", None),
        }

    async def process_all_contents(
        self,
        successful_results: List[Dict[str, Any]],
        custom_variables: Optional[Dict[str, Any]] = None,
        user_id: Optional[int] = None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
    ) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """
        批量处理文件内容.

        Args:
            successful_results: 成功处理的文件结果列表 (每项含 'content' / 'original_file')
            custom_variables:   自定义模板变量
            user_id:            用户 ID
            provider:           指定 LLM 提供商
            model:              指定模型

        Returns:
            元组 (final_report, file_processing_times)
            - final_report: 最后一个文件的分析报告 (与 v1 行为保持一致)
            - file_processing_times: 每个文件的耗时与费用信息列表
        """
        if not successful_results:
            self.log("没有成功处理的文件内容可分析", "warning")
            return None, []

        self.log(f"🚀 开始批量处理 {len(successful_results)} 个文件内容...")

        final_report: Optional[str] = None
        file_processing_times: List[Dict[str, Any]] = []

        for index, result in enumerate(successful_results):
            content = result.get("content", "")
            original_file = result.get("original_file", "")
            file_name = os.path.basename(original_file) if original_file else f"file_{index}"

            self.log(
                f"📄 处理第 {index + 1}/{len(successful_results)} 个文件: {file_name}"
            )
            self.log(f"   文件内容长度: {len(content)} 字符")

            try:
                analysis_result = await self.analyze_file_content(
                    file_content=content,
                    file_name=file_name,
                    custom_variables=custom_variables,
                    user_id=user_id,
                    provider=provider,
                    model=model,
                )

                # 与 v1 行为对齐: 仅最后一个文件的报告写入 final_report
                if index == len(successful_results) - 1:
                    final_report = analysis_result["report_content"]
                    self.log(f"📋 保存最终报告 (来自文件: {file_name})")

                file_processing_times.append({
                    "file": file_name,
                    "file_path": original_file,
                    "ai_time": analysis_result["response_time"],
                    "total_tokens": analysis_result["total_tokens"],
                    "total_price": analysis_result["total_price"],
                    "model_usage": analysis_result["model_usage"],
                    "model": analysis_result["model"],
                    "provider": analysis_result["provider"],
                })

                self.log(
                    f"✅ 文件 '{file_name}' 分析完成: "
                    f"耗时={analysis_result['response_time']:.2f}s, "
                    f"tokens={analysis_result['total_tokens']}, "
                    f"费用={analysis_result['total_price']:.6f} {analysis_result['currency']}"
                )

            except Exception as e:
                error_msg = f"文件 '{file_name}' 分析失败: {e}"
                self.log(error_msg, "error")
                file_processing_times.append({
                    "file": file_name,
                    "file_path": original_file,
                    "ai_time": 0,
                    "total_tokens": 0,
                    "total_price": 0,
                    "model_usage": [],
                    "error": str(e),
                })

        # 汇总统计
        total_tokens = sum(f.get("total_tokens", 0) for f in file_processing_times)
        total_price = sum(f.get("total_price", 0) for f in file_processing_times)
        total_time = sum(f.get("ai_time", 0) for f in file_processing_times)
        success_count = len([f for f in file_processing_times if "error" not in f])

        self.log(
            f"📊 批量处理完成: "
            f"成功={success_count}/{len(successful_results)}, "
            f"总时间={total_time:.2f}s, "
            f"总tokens={total_tokens}, "
            f"总费用={total_price:.6f}"
        )

        if not final_report:
            self.log("⚠️ 警告: 未能生成最终报告", "warning")

        return final_report, file_processing_times

    # ──────────────────────────────────────────────────────────────────
    # 模板信息查询 (保持兼容)
    # ──────────────────────────────────────────────────────────────────

    def get_template_info(self) -> Dict[str, Any]:
        """获取模板信息 (供调用方查看 prompt / 推荐配置)."""
        try:
            from backend.core.prompt_renderer import PromptRenderer
            renderer = PromptRenderer()
            template = renderer._load_template(
                template_id=self.template_id,
                template_code=self.template_code,
            )
            return template if template else {}
        except Exception as e:
            logger.error(f"获取模板信息失败: {e}")
            return {}
