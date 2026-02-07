#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
config.py
- .env を使わず、Python から設定を import して使うための共通設定ファイル
- 各スクリプトは `from config import CFG` で参照
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Config:
    # =========================================================
    # 基本パス（必須）
    # =========================================================
    DB_PATH: Path = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/list_category_gossip.db")
    TABLE_NAME: str = "items"
    BASE_OUTPUT_ROOT: Path = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001")
    SCRIPTS_DIR: Path = Path(r"/Users/yumahama/Documents/Python/07_動画生成_ガールズチャンネル/01_暫定_20260102")

    # =========================================================
    # ランチャー挙動（運用）
    # =========================================================
    RUNS_DEFAULT: int = 5
    STOP_ON_ERROR: bool = False
    SLEEP_SEC_WHEN_EMPTY: int = 0
    PASS_FOLDER_NAME_TO_05: bool = False

    # =========================================================
    # ステージ番号（check_create の進行管理）
    # =========================================================
    STA_02: int = 1
    END_02: int = 2
    STA_03: int = 2
    END_03: int = 3
    STA_04: int = 3
    END_04: int = 4
    STA_05: int = 4
    END_05: int = 5
    STA_99: int = 5
    END_99: int = 6

    # =========================================================
    # SQLite（安定稼働用）
    # =========================================================
    BUSY_TIMEOUT_MS: int = 60000
    SQLITE_JOURNAL_MODE: str = "WAL"
    SQLITE_SYNCHRONOUS: str = "NORMAL"
    LOCK_RETRY_MAX: int = 25
    LOCK_RETRY_SLEEP_SEC: float = 0.8

    # =========================================================
    # 02_データ取得（Playwright取得・整形）
    # =========================================================
    MAX_COMMENTS_TO_FETCH: int = 75
    HEADLESS_MODE: bool = True
    WAIT_TIMEOUT_MS: int = 45000
    REQUEST_INTERVAL_MS: int = 300
    FOLDER_TITLE_MAX_CHARS: int = 10
    ENABLE_EXCLUDE_BADWORDS: bool = True

    # =========================================================
    # 04_音声生成（VOICEVOX）
    # =========================================================
    ENGINE_URL: str = "http://127.0.0.1:50021"
    VOICE_BOOT_TIMEOUT_SEC: int = 60
    VOICE_POLL_INTERVAL_SEC: int = 2
    TOTAL_VIDEO_SEC: float = 45.0
    SILENCE_MS: int = 400
    MIN_SEC_PER_COMMENT: float = 2.0
    MAX_SEC_PER_COMMENT: float = 7.5

    # =========================================================
    # 05_サムネ/preview（イントロ音源）
    # =========================================================
    START_DIR: Path = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/start")
    START_MP3_NAME: str = "start.mp3"

    # =========================================================
    # 99_パーツ組み立て（最終出力）
    # =========================================================
    FPS: int = 30
    ENABLE_PREVIEW: bool = True
    PREVIEW_REL: Path = Path(r"image/preview/preview.mp4")
    ENABLE_ENDING: bool = True
    ENDING_IMAGE_PATH: Path = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/image/last_01.png")
    ENDING_AUDIO_PATH: Path = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/last/last.wav")
    ENABLE_BGM: bool = True
    BGM_ODD_PATH: Path = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/bgm/1.mp3")
    BGM_EVEN_PATH: Path = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/bgm/2.mp3")
    BGM_VOLUME: float = 0.12

    # =========================================================
    # 補助（スクリプト側が参照していたが、envに無いことも多いもの）
    # =========================================================
    PICK_ORDER: str = "post_date_desc"  # 01/03/04/05 のピック順


CFG = Config()
