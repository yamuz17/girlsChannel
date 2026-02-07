#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from config import CFG

# python-dotenv（入ってる前提）
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None


# =========================
# env helper
# =========================
def load_env(env_path: Optional[Path] = None) -> None:
    """
    互換用（実体は config.py が読み込み済み）
    """
    if load_dotenv is None:
        return
    if env_path is None:
        load_dotenv(override=False)
        return
    load_dotenv(dotenv_path=str(env_path), override=False)


def _env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return default if v is None else str(v)


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None:
        return int(default)
    s = str(v).strip().lower()
    if s in ("", "none", "null"):
        return int(default)
    return int(float(s))


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None:
        return float(default)
    s = str(v).strip().lower()
    if s in ("", "none", "null"):
        return float(default)
    return float(s)


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return bool(default)


def _env_optional_int(name: str, default: Optional[int]) -> Optional[int]:
    v = os.environ.get(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("", "none", "null"):
        return default
    try:
        return int(float(s))
    except Exception:
        return default


def _env_path(name: str, default: str = "") -> Path:
    s = _env_str(name, default).strip()
    return Path(s).expanduser()


def _env_required_path(name: str) -> Path:
    s = _env_str(name, "").strip()
    if not s:
        raise RuntimeError(f"env missing: {name}")
    return Path(s).expanduser()


def now_jst() -> str:
    return datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")


# =========================
# DB / Queue
# =========================
@dataclass(frozen=True)
class QueueConfig:
    db_path: Path
    table: str
    base_output_root: Path

    busy_timeout_ms: int = 60000
    sqlite_wal: bool = True
    sqlite_synchronous: str = "NORMAL"  # NORMAL/OFF/FULL 等

    # pick高速化（運用でON/OFF）
    enable_pick_queue_index: bool = True
    pick_queue_index_name: str = "idx_items_pick_queue"


def build_queue_config_from_env() -> QueueConfig:
    # 運用上必須：DB_PATH / BASE_OUTPUT_ROOT
    db_path = CFG.DB_PATH
    base_output_root = CFG.BASE_OUTPUT_ROOT

    table = (CFG.TABLE_NAME or "items").strip() or "items"

    busy = CFG.BUSY_TIMEOUT_MS
    wal = CFG.SQLITE_WAL
    sync = (CFG.SQLITE_SYNCHRONOUS or "NORMAL").strip().upper() or "NORMAL"

    enable_idx = CFG.ENABLE_PICK_QUEUE_INDEX
    idx_name = (CFG.PICK_QUEUE_INDEX_NAME or "idx_items_pick_queue").strip() or "idx_items_pick_queue"

    return QueueConfig(
        db_path=db_path,
        table=table,
        base_output_root=base_output_root,
        busy_timeout_ms=busy,
        sqlite_wal=wal,
        sqlite_synchronous=sync,
        enable_pick_queue_index=enable_idx,
        pick_queue_index_name=idx_name,
    )


def connect_db(cfg: QueueConfig) -> sqlite3.Connection:
    if not cfg.db_path.exists():
        raise FileNotFoundError(f"DB not found: {cfg.db_path}")

    con = sqlite3.connect(str(cfg.db_path), timeout=cfg.busy_timeout_ms / 1000)
    con.row_factory = sqlite3.Row

    if cfg.sqlite_wal:
        con.execute("PRAGMA journal_mode=WAL;")
    con.execute(f"PRAGMA synchronous={cfg.sqlite_synchronous};")
    con.execute(f"PRAGMA busy_timeout={int(cfg.busy_timeout_ms)};")
    return con


def ensure_common_columns(con: sqlite3.Connection, table: str) -> None:
    cols = {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}

    def add(sql: str) -> None:
        try:
            con.execute(sql)
        except Exception:
            pass

    # キュー制御
    if "check_create" not in cols:
        add(f"ALTER TABLE {table} ADD COLUMN check_create INTEGER DEFAULT 0")
    if "folder_name" not in cols:
        add(f"ALTER TABLE {table} ADD COLUMN folder_name TEXT")
    if "last_error" not in cols:
        add(f"ALTER TABLE {table} ADD COLUMN last_error TEXT")
    if "updated_at" not in cols:
        add(f"ALTER TABLE {table} ADD COLUMN updated_at TEXT")

    # 進捗フラグ（既存スクリプト群と統一）
    if "video_created" not in cols:
        add(f"ALTER TABLE {table} ADD COLUMN video_created INTEGER DEFAULT 0")
    if "video_created_at" not in cols:
        add(f"ALTER TABLE {table} ADD COLUMN video_created_at TEXT")
    if "video_uploaded" not in cols:
        add(f"ALTER TABLE {table} ADD COLUMN video_uploaded INTEGER DEFAULT 0")
    if "video_uploaded_at" not in cols:
        add(f"ALTER TABLE {table} ADD COLUMN video_uploaded_at TEXT")

    con.commit()


def ensure_pick_queue_index(con: sqlite3.Connection, cfg: QueueConfig) -> None:
    """
    pickの高速化（任意）
    - table / columns を前提に安全寄りに作る
    """
    if not cfg.enable_pick_queue_index:
        return

    # SQL自体は壊れやすいので “コード側” で生成（あなたの方針に合わせる）
    # check_create で絞って、次に comments_count を使うケースが多い
    sql = f"CREATE INDEX IF NOT EXISTS {cfg.pick_queue_index_name} ON {cfg.table}(check_create, comments_count DESC, post_date ASC)"
    try:
        con.execute(sql)
        con.commit()
    except Exception:
        # 環境差で post_date/comments_count が無い場合は落とさない
        pass


def pick_one(con: sqlite3.Connection, table: str, sta: int, pick_order: str) -> Optional[Tuple[int, str]]:
    """
    check_create==sta を1件拾う。返り値: (id, folder_name)
    """
    order_sql = "id DESC"
    if pick_order == "post_date_desc":
        order_sql = "post_date DESC, id DESC"
    elif pick_order == "comments_desc":
        order_sql = "comments_count DESC, post_date DESC, id DESC"

    row = con.execute(
        f"""
        SELECT id, folder_name
          FROM {table}
         WHERE check_create = ?
           AND folder_name IS NOT NULL
           AND folder_name != ''
         ORDER BY {order_sql}
         LIMIT 1
        """,
        (int(sta),),
    ).fetchone()

    if not row:
        return None
    return int(row["id"]), str(row["folder_name"])


def mark_done(con: sqlite3.Connection, table: str, item_id: int, sta_expected: int, end_value: int) -> None:
    """
    成功：STA→END（期待値付き）
    """
    cur = con.execute(
        f"""
        UPDATE {table}
           SET check_create = ?,
               last_error   = NULL,
               updated_at   = ?
         WHERE id = ?
           AND check_create = ?
        """,
        (int(end_value), now_jst(), int(item_id), int(sta_expected)),
    )
    con.commit()

    if cur.rowcount == 0:
        raise RuntimeError(
            f"mark_done rowcount=0 (id={item_id}) : check_create が想定STA({sta_expected})ではない可能性"
        )


def mark_fail(con: sqlite3.Connection, table: str, item_id: int, sta_value: int, err: str) -> None:
    """
    失敗：STA据え置き + last_error
    """
    con.execute(
        f"""
        UPDATE {table}
           SET check_create = ?,
               last_error   = ?,
               updated_at   = ?
         WHERE id = ?
        """,
        (int(sta_value), str(err)[:2000], now_jst(), int(item_id)),
    )
    con.commit()


def mark_video_created(con: sqlite3.Connection, table: str, item_id: int) -> None:
    con.execute(
        f"""
        UPDATE {table}
           SET video_created    = 1,
               video_created_at = ?,
               updated_at       = ?
         WHERE id = ?
        """,
        (now_jst(), now_jst(), int(item_id)),
    )
    con.commit()
