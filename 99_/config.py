#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import env_loader


# 一度だけ env を読み込む
_ENV_PATH = env_loader.load_env()


def _env_str(name: str, default: str = "") -> str:
    return env_loader.env_str(name, default) or default


def _env_int(name: str, default: int) -> int:
    return int(env_loader.env_int(name, default))


def _env_float(name: str, default: float) -> float:
    return float(env_loader.env_float(name, default))


def _env_bool(name: str, default: bool) -> bool:
    return bool(env_loader.env_bool(name, default))


def _env_path(name: str, default: Optional[str]) -> Optional[Path]:
    return env_loader.env_path(name, default)


def _env_opt_int(name: str, default: Optional[int]) -> Optional[int]:
    return env_loader.env_optional_int(name, default)


def _env_opt_timeout(*keys: str, default: Optional[int] = None) -> Optional[int]:
    for k in keys:
        v = _env_opt_int(k, None)
        if v is not None:
            return v
    return default


@dataclass(frozen=True)
class Config:
    # --- base paths ---
    DB_PATH: Path
    TABLE_NAME: str
    BASE_OUTPUT_ROOT: Path
    SCRIPTS_DIR: Path

    # --- scripts ---
    SCRIPT_LIST_NAME: str
    SCRIPT_02_NAME: str
    SCRIPT_03_NAME: str
    SCRIPT_04_NAME: str
    SCRIPT_05_NAME: str
    SCRIPT_99_NAME: str
    SCRIPT_SCHEDULE_NAME: str

    # --- launcher behavior ---
    RUNS_DEFAULT: int
    STOP_ON_ERROR: bool
    RESET_TO_ZERO_ON_FAIL_02: bool
    SLEEP_SEC_WHEN_EMPTY: float
    PASS_FOLDER_NAME_TO_05: bool

    # --- stages ---
    STA_02: int
    END_02: int
    STA_03: int
    END_03: int
    STA_04: int
    END_04: int
    STA_05: int
    END_05: int
    STA_99: int
    END_99: int

    # --- sqlite ---
    BUSY_TIMEOUT_MS: int
    SQLITE_WAL: bool
    SQLITE_JOURNAL_MODE: str
    SQLITE_SYNCHRONOUS: str
    LOCK_RETRY_MAX: int
    LOCK_RETRY_SLEEP_SEC: float
    ENABLE_PICK_QUEUE_INDEX: bool
    PICK_QUEUE_INDEX_NAME: str

    # --- timeouts ---
    TIMEOUT_02: Optional[int]
    TIMEOUT_03: Optional[int]
    TIMEOUT_04: Optional[int]
    TIMEOUT_05: Optional[int]
    TIMEOUT_99: Optional[int]
    TIMEOUT_LIST: Optional[int]
    TIMEOUT_SCHEDULE: Optional[int]

    # --- upload ---
    API_DIR: Path
    CLIENT_JSON_NAME: str
    TOKEN_NAME: str

    @property
    def ENV_PATH(self) -> str:
        return str(_ENV_PATH)


CFG = Config(
    DB_PATH=_env_path("DB_PATH", None) or Path(),
    TABLE_NAME=_env_str("TABLE_NAME", "items") or "items",
    BASE_OUTPUT_ROOT=_env_path("BASE_OUTPUT_ROOT", None) or Path(),
    SCRIPTS_DIR=_env_path("SCRIPTS_DIR", str(Path(__file__).resolve().parent))
    or Path(__file__).resolve().parent,

    SCRIPT_LIST_NAME=_env_str("SCRIPT_LIST_NAME", "build_list.py") or "build_list.py",
    SCRIPT_02_NAME=_env_str("SCRIPT_02_NAME", "fetch_data.py") or "fetch_data.py",
    SCRIPT_03_NAME=_env_str("SCRIPT_03_NAME", "make_images.py") or "make_images.py",
    SCRIPT_04_NAME=_env_str("SCRIPT_04_NAME", "make_audio.py") or "make_audio.py",
    SCRIPT_05_NAME=_env_str("SCRIPT_05_NAME", "make_preview.py") or "make_preview.py",
    SCRIPT_99_NAME=_env_str("SCRIPT_99_NAME", "assemble_video.py") or "assemble_video.py",
    SCRIPT_SCHEDULE_NAME=_env_str("SCRIPT_SCHEDULE_NAME", "投稿予約.py") or "投稿予約.py",

    RUNS_DEFAULT=_env_int("RUNS_DEFAULT", 1),
    STOP_ON_ERROR=_env_bool("STOP_ON_ERROR", False),
    RESET_TO_ZERO_ON_FAIL_02=_env_bool("RESET_TO_ZERO_ON_FAIL_02", False),
    SLEEP_SEC_WHEN_EMPTY=float(_env_float("SLEEP_SEC_WHEN_EMPTY", 0.0)),
    PASS_FOLDER_NAME_TO_05=_env_bool("PASS_FOLDER_NAME_TO_05", False),

    STA_02=_env_int("STA_02", 1),
    END_02=_env_int("END_02", 2),
    STA_03=_env_int("STA_03", 2),
    END_03=_env_int("END_03", 3),
    STA_04=_env_int("STA_04", 3),
    END_04=_env_int("END_04", 4),
    STA_05=_env_int("STA_05", 4),
    END_05=_env_int("END_05", 5),
    STA_99=_env_int("STA_99", 5),
    END_99=_env_int("END_99", 6),

    BUSY_TIMEOUT_MS=_env_int("BUSY_TIMEOUT_MS", 60000),
    SQLITE_WAL=_env_bool("SQLITE_WAL", True),
    SQLITE_JOURNAL_MODE=(_env_str("SQLITE_JOURNAL_MODE", "WAL") or "WAL").strip(),
    SQLITE_SYNCHRONOUS=(_env_str("SQLITE_SYNCHRONOUS", "NORMAL") or "NORMAL").strip(),
    LOCK_RETRY_MAX=_env_int("LOCK_RETRY_MAX", 25),
    LOCK_RETRY_SLEEP_SEC=float(_env_float("LOCK_RETRY_SLEEP_SEC", 0.8)),
    ENABLE_PICK_QUEUE_INDEX=_env_bool("ENABLE_PICK_QUEUE_INDEX", True),
    PICK_QUEUE_INDEX_NAME=_env_str("PICK_QUEUE_INDEX_NAME", "idx_items_pick_queue")
    or "idx_items_pick_queue",

    TIMEOUT_02=_env_opt_timeout("TIMEOUT_02", default=None),
    TIMEOUT_03=_env_opt_timeout("TIMEOUT_03", default=None),
    TIMEOUT_04=_env_opt_timeout("TIMEOUT_04", default=None),
    TIMEOUT_05=_env_opt_timeout("TIMEOUT_05_THUMB", "TIMEOUT_05", default=None),
    TIMEOUT_99=_env_opt_timeout("TIMEOUT_99", "TIMEOUT_98", default=None),
    TIMEOUT_LIST=_env_opt_timeout("TIMEOUT_LIST", default=None),
    TIMEOUT_SCHEDULE=_env_opt_timeout("TIMEOUT_SCHEDULE", default=None),

    API_DIR=_env_path("API_DIR", None) or Path(),
    CLIENT_JSON_NAME=_env_str(
        "CLIENT_JSON_NAME",
        "client_secret_769746086148-u7ma3j71951biaee4anijl2prgr1n9l4.apps.googleusercontent.com.json",
    ),
    TOKEN_NAME=_env_str("TOKEN_NAME", "token.json"),
)
