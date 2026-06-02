# -*- coding: utf-8 -*-
"""
Chat group message persistence service.

Normalizes group messages from Lark callbacks and external callers into the
`chat_group_messages` table.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2.extras

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import UTC, ensure_utc
from backend.utils.logger import get_logger

logger = get_logger("chat_group_messages_service")

URL_RE = re.compile(r"https?://[^\s\]\)\"']+")


class ChatGroupMessagesService:
    """Service for normalizing and storing chat group messages."""

    def __init__(self):
        self.db_manager = DatabaseManager(get_database_config())

    def save_lark_callback(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Persist one Lark callback payload if it is a group message."""
        message = payload.get("event", {}).get("message", {})
        chat_type = message.get("chat_type")

        if chat_type != "group":
            return {
                "processed": 0,
                "inserted": 0,
                "updated": 0,
                "ignored": 1,
                "reason": f"unsupported chat_type: {chat_type}",
            }

        record = self.normalize_lark_payload(payload)
        result = self.save_messages([record])
        result["ignored"] = 0
        return result

    def normalize_lark_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a Lark message webhook payload to table fields."""
        event = payload.get("event", {})
        message = event.get("message", {})
        sender = event.get("sender", {})
        sender_id = sender.get("sender_id") or {}

        content_fields = handle_lark_message(payload)
        return {
            "source": "lark",
            "message_id": message.get("message_id"),
            "chat_id": message.get("chat_id"),
            "chat_name": message.get("chat_name") or message.get("name"),
            "sender_id": (
                sender_id.get("open_id")
                or sender_id.get("user_id")
                or sender_id.get("union_id")
                or sender.get("sender_id")
            ),
            "sender_name": sender.get("sender_name") or sender.get("name"),
            "send_time": self._parse_lark_time(message.get("create_time")),
            **content_fields,
        }

    def save_messages(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Batch upsert messages into `chat_group_messages`.

        Records with a non-null `(source, message_id)` are idempotent; repeated
        callbacks update the existing row.
        """
        normalized_records = []
        skipped = 0
        for message in messages:
            normalized = self._normalize_external_message(message)
            if normalized:
                normalized_records.append(normalized)
            else:
                skipped += 1

        if not normalized_records:
            return {"processed": 0, "inserted": 0, "updated": 0, "skipped": skipped}

        total_before_dedup = len(normalized_records)
        normalized_records = self._dedupe_batch(normalized_records)
        duplicates_removed = total_before_dedup - len(normalized_records)

        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                raise RuntimeError("无法获取数据库连接")

            cursor = conn.cursor()
            self._ensure_table(cursor)

            insert_sql = """
            INSERT INTO chat_group_messages (
                source, message_id, chat_id, chat_name, sender_id, sender_name,
                send_time, msg_type, content, link_path, file_path
            )
            VALUES %s
            ON CONFLICT (source, message_id) WHERE message_id IS NOT NULL
            DO UPDATE SET
                chat_id = EXCLUDED.chat_id,
                chat_name = EXCLUDED.chat_name,
                sender_id = EXCLUDED.sender_id,
                sender_name = EXCLUDED.sender_name,
                send_time = EXCLUDED.send_time,
                msg_type = EXCLUDED.msg_type,
                content = EXCLUDED.content,
                link_path = EXCLUDED.link_path,
                file_path = EXCLUDED.file_path,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted_flag;
            """
            values = [
                (
                    record["source"],
                    record.get("message_id"),
                    record["chat_id"],
                    record.get("chat_name"),
                    record["sender_id"],
                    record.get("sender_name"),
                    record["send_time"],
                    record["msg_type"],
                    record.get("content"),
                    record.get("link_path"),
                    record.get("file_path"),
                )
                for record in normalized_records
            ]

            psycopg2.extras.execute_values(
                cursor,
                insert_sql,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                page_size=100,
            )
            results = cursor.fetchall()
            conn.commit()

            inserted = sum(1 for row in results if row and row[0])
            processed = len(results)
            return {
                "processed": processed,
                "inserted": inserted,
                "updated": processed - inserted,
                "skipped": skipped,
                "duplicates_removed": duplicates_removed,
            }
        except Exception as exc:
            if conn:
                conn.rollback()
            logger.error(f"保存群消息失败: {exc}")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _ensure_table(self, cursor) -> None:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_group_messages (
                id BIGSERIAL PRIMARY KEY,
                source VARCHAR(50) NOT NULL,
                message_id VARCHAR(255),
                chat_id VARCHAR(255) NOT NULL,
                chat_name VARCHAR(255),
                sender_id VARCHAR(255) NOT NULL,
                sender_name VARCHAR(255),
                send_time TIMESTAMP(6) NOT NULL,
                msg_type VARCHAR(50) NOT NULL,
                content TEXT,
                link_path VARCHAR(1000),
                file_path VARCHAR(500),
                created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uk_chat_group_messages_source_message_id
            ON chat_group_messages (source, message_id)
            WHERE message_id IS NOT NULL
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chat_group_messages_source ON chat_group_messages (source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chat_group_messages_chat_id ON chat_group_messages (chat_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chat_group_messages_sender_id ON chat_group_messages (sender_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chat_group_messages_msg_type ON chat_group_messages (msg_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_chat_group_messages_send_time ON chat_group_messages (send_time DESC)")

    def _normalize_external_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        source = str(message.get("source") or "external").strip()
        chat_id = str(message.get("chat_id") or "").strip()
        sender_id = str(message.get("sender_id") or "").strip()
        msg_type = str(message.get("msg_type") or "text").strip()
        send_time = self._parse_datetime(message.get("send_time"))

        if not source or not chat_id or not sender_id or not msg_type or not send_time:
            logger.warning("跳过无效群消息记录", message_id=message.get("message_id"), chat_id=chat_id)
            return None

        return {
            "source": source,
            "message_id": self._optional_str(message.get("message_id")),
            "chat_id": chat_id,
            "chat_name": self._optional_str(message.get("chat_name")),
            "sender_id": sender_id,
            "sender_name": self._optional_str(message.get("sender_name")),
            "send_time": send_time,
            "msg_type": msg_type,
            "content": self._optional_str(message.get("content")),
            "link_path": self._optional_str(message.get("link_path")),
            "file_path": self._optional_str(message.get("file_path")),
        }

    def _dedupe_batch(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Avoid PostgreSQL ON CONFLICT touching the same row twice in one batch."""
        keyed_records: Dict[tuple[str, str], Dict[str, Any]] = {}
        records_without_message_id: List[Dict[str, Any]] = []

        for record in records:
            message_id = record.get("message_id")
            if not message_id:
                records_without_message_id.append(record)
                continue
            keyed_records[(record["source"], message_id)] = record

        return records_without_message_id + list(keyed_records.values())

    def _parse_lark_time(self, raw_value: Any) -> datetime:
        if raw_value is None:
            return datetime.now(UTC)
        try:
            timestamp_ms = int(raw_value)
            return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        except (TypeError, ValueError):
            logger.warning(f"Lark create_time 解析失败，使用当前时间: {raw_value}")
            return datetime.now(UTC)

    def _parse_datetime(self, raw_value: Any) -> Optional[datetime]:
        if raw_value is None:
            return None
        if isinstance(raw_value, datetime):
            return ensure_utc(raw_value)
        if isinstance(raw_value, (int, float)):
            seconds = raw_value / 1000 if raw_value > 10_000_000_000 else raw_value
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        if isinstance(raw_value, str):
            value = raw_value.strip()
            if not value:
                return None
            if value.isdigit():
                return self._parse_datetime(int(value))
            try:
                return ensure_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
            except ValueError:
                return None
        return None

    def _optional_str(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value)
        return text if text != "" else None


def handle_lark_message(payload: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Return normalized msg_type/content/link_path/file_path fields."""
    message = payload.get("event", {}).get("message", {})
    raw_message_type = message.get("message_type")
    content_json = _parse_content(message.get("content"))

    if raw_message_type == "text":
        return _handle_text(content_json)

    if raw_message_type == "image":
        image_key = content_json.get("image_key")
        return {
            "msg_type": "image",
            "content": "[图片]",
            "link_path": None,
            "file_path": str(image_key) if image_key else None,
        }

    if raw_message_type == "post":
        return _handle_post(content_json)

    if raw_message_type in {"file", "folder", "audio", "media", "sticker"}:
        return _handle_file_like(content_json)

    return {
        "msg_type": "其他",
        "content": _fallback_content_text(content_json),
        "link_path": None,
        "file_path": None,
    }


def _parse_content(raw_content: Any) -> Dict[str, Any]:
    if isinstance(raw_content, dict):
        return raw_content
    if not raw_content:
        return {}
    if isinstance(raw_content, str):
        try:
            parsed = json.loads(raw_content)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {"text": raw_content}
    return {}


def _handle_text(content_json: Dict[str, Any]) -> Dict[str, Optional[str]]:
    text = str(content_json.get("text") or "")
    first_url = _first_url(text)
    return {
        "msg_type": "link" if first_url else "text",
        "content": text,
        "link_path": first_url,
        "file_path": None,
    }


def _handle_post(content_json: Dict[str, Any]) -> Dict[str, Optional[str]]:
    lines: List[str] = []
    links: List[str] = []
    files: List[str] = []
    has_readable_text = False

    for block in content_json.get("content") or []:
        if not isinstance(block, list):
            continue

        parts: List[str] = []
        for item in block:
            if not isinstance(item, dict):
                continue

            text_part = _post_item_to_text(item)
            if text_part:
                parts.append(text_part)
                if item.get("tag") in {"text", "a", "at", "code_block"}:
                    has_readable_text = True

            link = _post_item_link(item)
            if link:
                links.append(link)

            file_key = _post_item_file_key(item)
            if file_key:
                files.append(file_key)

        if parts:
            lines.append("".join(parts))

    content = "\n".join(lines)
    link_path = links[0] if links else _first_url(content)
    file_path = files[0] if files else None

    if has_readable_text:
        msg_type = "text"
    elif file_path and file_path.startswith("img_"):
        msg_type = "image"
    elif link_path:
        msg_type = "link"
    elif file_path:
        msg_type = "file"
    else:
        msg_type = "其他"

    return {
        "msg_type": msg_type,
        "content": content,
        "link_path": link_path,
        "file_path": file_path,
    }


def _post_item_to_text(item: Dict[str, Any]) -> str:
    tag = item.get("tag")
    if tag == "text":
        return str(item.get("text") or "")
    if tag == "a":
        return str(item.get("text") or item.get("href") or "")
    if tag == "at":
        return "@" + str(item.get("user_name") or item.get("user_id") or "")
    if tag == "img":
        return "[图片]"
    if tag == "media":
        return "[视频]"
    if tag in {"file", "folder"}:
        return "[文件]"
    if tag == "emotion":
        emoji_type = item.get("emoji_type")
        return f"[表情:{emoji_type}]" if emoji_type else "[表情]"
    if tag == "hr":
        return "----"
    if tag == "code_block":
        return str(item.get("text") or "")
    return ""


def _post_item_link(item: Dict[str, Any]) -> Optional[str]:
    if item.get("tag") == "a" and item.get("href"):
        return str(item["href"])
    text = item.get("text")
    return _first_url(text) if isinstance(text, str) else None


def _post_item_file_key(item: Dict[str, Any]) -> Optional[str]:
    for key in ("image_key", "file_key"):
        value = item.get(key)
        if value:
            return str(value)
    return None


def _handle_file_like(content_json: Dict[str, Any]) -> Dict[str, Optional[str]]:
    file_path = content_json.get("file_key") or content_json.get("image_key")
    file_name = content_json.get("file_name")

    if file_name:
        content = str(file_name)
    elif content_json.get("duration"):
        content = "[音视频]"
    else:
        content = "[文件]"

    return {
        "msg_type": "file",
        "content": content,
        "link_path": None,
        "file_path": str(file_path) if file_path else None,
    }


def _fallback_content_text(content_json: Dict[str, Any]) -> str:
    for key in ("text", "title", "summary", "topic", "content"):
        value = content_json.get(key)
        if isinstance(value, str):
            return value
    return json.dumps(content_json, ensure_ascii=False) if content_json else ""


def _first_url(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    match = URL_RE.search(text)
    return match.group(0) if match else None
