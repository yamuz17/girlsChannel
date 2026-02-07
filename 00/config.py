#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def _default_db_path() -> Path:
    return Path.home() / "Documents" / "pythonOutput" / "list_category_gossip.db"


def load_config() -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "DB_PATH": str(_default_db_path()),
    }

    local_path = Path(__file__).resolve().parent / "config.local.json"
    if local_path.exists():
        try:
            data = json.loads(local_path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                cfg.update(data)
        except Exception:
            pass

    return cfg


CFG = load_config()
DB_PATH = Path(CFG["DB_PATH"]).expanduser()
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
