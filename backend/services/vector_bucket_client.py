# -*- coding: utf-8 -*-
"""
阿里云 OSS Vector Bucket 客户端封装。

职责：
- 把 alibabacloud-oss-python-sdk-v2 的 vectors API 收口到本仓库的服务层；
- 暴露 put_vectors / query_vectors / delete_vectors / list_vectors_by_filter 四个高层方法；
- 提供单例懒加载（避免每次写入都重新建立 client），同时支持 force re-init 测试覆盖。

设计要点：
- 与既有 oss2 客户端并存。oss2 用于 Standard Bucket（原文件），本模块仅服务 Vector Bucket。
- 缺失配置时返回 None 客户端并打 warning，不抛异常——便于在未配置 Vector Bucket
  的开发环境跑通其它链路。调用方负责判空。
- 所有方法接受 Python 原生 dict/list；不暴露 alibabacloud_oss_v2 类型。
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from backend.utils.env_loader import load_project_env
from backend.utils.logger import get_logger

load_project_env()
logger = get_logger("vector_bucket_client")


@dataclass(frozen=True)
class VectorRecord:
    """单条向量入库记录。"""

    key: str
    vector: Sequence[float]
    metadata: Dict[str, str]


@dataclass(frozen=True)
class VectorHit:
    """单条向量查询命中。"""

    key: str
    distance: Optional[float]
    metadata: Dict[str, str]


class VectorBucketError(RuntimeError):
    """Vector Bucket 调用失败。上层任务捕获后写入 vector_error。"""


class VectorBucketClient:
    """OSS Vector Bucket 操作客户端。"""

    PUT_BATCH_MAX = 100  # 阿里云单次 PutVectors 推荐上限
    DELETE_BATCH_MAX = 100

    def __init__(
        self,
        bucket: Optional[str] = None,
        index_name: Optional[str] = None,
        region: Optional[str] = None,
        account_id: Optional[str] = None,
        endpoint: Optional[str] = None,
    ) -> None:
        self.bucket = bucket or os.getenv("OSS_VECTOR_BUCKET", "").strip()
        self.index_name = index_name or os.getenv("OSS_VECTOR_INDEX", "contentchunks").strip()
        self.region = region or os.getenv("OSS_VECTOR_REGION", "cn-hongkong").strip()
        self.account_id = account_id or os.getenv("OSS_VECTOR_ACCOUNT_ID", "").strip()
        self.endpoint = endpoint or os.getenv("OSS_VECTOR_ENDPOINT", "").strip()
        self._client = None  # type: Any
        self._sdk_models = None  # type: Any
        self._init_lock = threading.Lock()

    # ───────────────────────── 内部：lazy SDK init ────────────────────────
    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        with self._init_lock:
            if self._client is not None:
                return self._client
            if not self.bucket:
                logger.warning(
                    "OSS_VECTOR_BUCKET 未配置，Vector Bucket client 未初始化。"
                    "向量化功能将被跳过。"
                )
                return None
            try:
                # alibabacloud-oss-v2 (PyPI package name)
                import alibabacloud_oss_v2 as oss  # type: ignore
                import alibabacloud_oss_v2.vectors as oss_vectors  # type: ignore
                import alibabacloud_oss_v2.vectors.models as oss_vec_models  # type: ignore
            except ImportError as exc:
                logger.error(f"alibabacloud-oss-v2 未安装，无法访问 Vector Bucket: {exc}")
                raise VectorBucketError(
                    "alibabacloud-oss-v2 not installed"
                ) from exc

            # 直接用 OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET（与 Standard Bucket 共用同一 AK）
            ak_id = os.getenv("OSS_ACCESS_KEY_ID", "")
            ak_secret = os.getenv("OSS_ACCESS_KEY_SECRET", "")
            if not ak_id or not ak_secret:
                raise VectorBucketError(
                    "OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET not set"
                )
            credentials_provider = oss.credentials.StaticCredentialsProvider(
                access_key_id=ak_id,
                access_key_secret=ak_secret,
            )

            cfg = oss.config.load_default()
            cfg.credentials_provider = credentials_provider
            cfg.region = self.region
            if self.account_id:
                cfg.account_id = self.account_id
            if self.endpoint:
                cfg.endpoint = self.endpoint

            self._client = oss_vectors.Client(cfg)
            self._sdk_models = oss_vec_models
            logger.info(
                f"Vector Bucket client initialised: bucket={self.bucket}, "
                f"index={self.index_name}, region={self.region}"
            )
            return self._client

    @property
    def available(self) -> bool:
        """True 表示客户端已配置且 SDK 已就绪。"""
        try:
            return self._ensure_client() is not None
        except VectorBucketError:
            return False

    # ─────────────────────────── PutVectors ──────────────────────────────
    def put_vectors(self, records: Sequence[VectorRecord]) -> int:
        """批量写入向量。返回成功条数。失败抛 VectorBucketError。"""
        client = self._ensure_client()
        if client is None or not records:
            return 0

        written = 0
        batches = self._chunked(records, self.PUT_BATCH_MAX)
        for batch in batches:
            vectors_payload: List[Dict[str, Any]] = []
            for r in batch:
                if not r.key or not r.vector:
                    continue
                vectors_payload.append(
                    {
                        "key": r.key,
                        "data": {"float32": list(r.vector)},
                        "metadata": {k: str(v) for k, v in (r.metadata or {}).items()},
                    }
                )
            if not vectors_payload:
                continue

            try:
                req = self._sdk_models.PutVectorsRequest(
                    bucket=self.bucket,
                    index_name=self.index_name,
                    vectors=vectors_payload,
                )
                client.put_vectors(req)
                written += len(vectors_payload)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    f"PutVectors failed: bucket={self.bucket} index={self.index_name} "
                    f"batch_size={len(vectors_payload)} err={exc}"
                )
                raise VectorBucketError(f"PutVectors failed: {exc}") from exc

        return written

    # ─────────────────────────── QueryVectors ────────────────────────────
    def query_vectors(
        self,
        query_vector: Sequence[float],
        top_k: int = 10,
        filter_expr: Optional[Dict[str, Any]] = None,
        return_metadata: bool = True,
        return_distance: bool = True,
    ) -> List[VectorHit]:
        """相似度检索；返回 VectorHit 列表（按距离升序）。"""
        client = self._ensure_client()
        if client is None:
            return []

        if not 1 <= top_k <= 100:
            top_k = max(1, min(100, top_k))

        try:
            req_kwargs: Dict[str, Any] = {
                "bucket": self.bucket,
                "index_name": self.index_name,
                "query_vector": {"float32": list(query_vector)},
                "top_k": top_k,
                "return_metadata": return_metadata,
                "return_distance": return_distance,
            }
            if filter_expr:
                req_kwargs["filter"] = filter_expr
            req = self._sdk_models.QueryVectorsRequest(**req_kwargs)
            res = client.query_vectors(req)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                f"QueryVectors failed: bucket={self.bucket} index={self.index_name} "
                f"top_k={top_k} err={exc}"
            )
            raise VectorBucketError(f"QueryVectors failed: {exc}") from exc

        hits: List[VectorHit] = []
        raw_vectors = getattr(res, "vectors", None) or []
        for v in raw_vectors:
            # SDK 返回项可能是 dict 也可能是带属性的对象
            if isinstance(v, dict):
                key = v.get("key") or v.get("Key") or ""
                distance = v.get("distance")
                metadata = v.get("metadata") or {}
            else:
                key = getattr(v, "key", "") or ""
                distance = getattr(v, "distance", None)
                metadata = getattr(v, "metadata", {}) or {}
            if not key:
                continue
            hits.append(
                VectorHit(
                    key=str(key),
                    distance=float(distance) if distance is not None else None,
                    metadata={k: str(val) for k, val in (metadata or {}).items()},
                )
            )
        return hits

    # ─────────────────────────── DeleteVectors ───────────────────────────
    def delete_vectors(self, keys: Sequence[str]) -> int:
        """按 key 批量删除向量。返回请求删除的条数（不保证服务端真的删了多少）。"""
        client = self._ensure_client()
        if client is None or not keys:
            return 0

        deleted = 0
        for batch in self._chunked(keys, self.DELETE_BATCH_MAX):
            keys_clean = [str(k) for k in batch if k]
            if not keys_clean:
                continue
            try:
                req = self._sdk_models.DeleteVectorsRequest(
                    bucket=self.bucket,
                    index_name=self.index_name,
                    keys=keys_clean,
                )
                client.delete_vectors(req)
                deleted += len(keys_clean)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    f"DeleteVectors failed: bucket={self.bucket} index={self.index_name} "
                    f"batch_size={len(keys_clean)} err={exc}"
                )
                raise VectorBucketError(f"DeleteVectors failed: {exc}") from exc

        return deleted

    # ─────────────────────────── helpers ─────────────────────────────────
    @staticmethod
    def _chunked(items: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
        for i in range(0, len(items), size):
            yield items[i : i + size]


# 进程内单例
_default_client: Optional[VectorBucketClient] = None
_default_lock = threading.Lock()


def get_default_client() -> VectorBucketClient:
    """返回进程级单例（懒加载）。"""
    global _default_client
    if _default_client is not None:
        return _default_client
    with _default_lock:
        if _default_client is None:
            _default_client = VectorBucketClient()
    return _default_client
