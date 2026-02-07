#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

from pathlib import Path
import env_loader

# =========================================================
# .env 読み込み（このスクリプトと同階層の .env）
# =========================================================
SCRIPT_DIR = Path(__file__).resolve().parent
_ENV_PATH = env_loader.load_env(SCRIPT_DIR / ".env")  # ← path を渡す



# =========================================================
# env helper（互換キー吸収）
# =========================================================
def _env_str(key: str, default: str = "") -> str:
    return (env_loader.env_str(key, default) or default).strip()


def _env_bool(key: str, default: bool) -> bool:
    return bool(env_loader.env_bool(key, default))


def _env_int(key: str, default: int) -> int:
    return int(env_loader.env_int(key, default))


def _env_float(key: str, default: float) -> float:
    return float(env_loader.env_float(key, default))


def _env_opt_int(key: str, default: Optional[int]) -> Optional[int]:
    return env_loader.env_optional_int(key, default)


def _env_path(key: str, default: Optional[str]) -> Optional[Path]:
    p = env_loader.env_path(key, default)
    return p


def _env_opt_timeout(*keys: str, default: Optional[int] = None) -> Optional[int]:
    """
    TIMEOUT_05_THUMB / TIMEOUT_05 など、どちらかが設定されていれば採用する。
    """
    for k in keys:
        v = _env_opt_int(k, None)
        if v is not None:
            return v
    return default


# =========================================================
# 設定（運用系は env、ロジック系はコード）
# =========================================================

# --- DB（運用：env）---
DB_PATH = _env_path("DB_PATH", None)
if DB_PATH is None:
    raise SystemExit("[ENV] missing required key: DB_PATH")

TABLE_NAME = _env_str("TABLE_NAME", "items") or "items"

# ★新規(0)の取得順（ロジック寄りなのでコードに残す）
PICK_NEW_ORDER_SQL = "post_date DESC, id DESC"

# --- pick高速化インデックス（運用寄り：env）---
ENABLE_PICK_QUEUE_INDEX = _env_bool("ENABLE_PICK_QUEUE_INDEX", True)
PICK_QUEUE_INDEX_NAME = _env_str("PICK_QUEUE_INDEX_NAME", "idx_items_pick_queue") or "idx_items_pick_queue"
PICK_QUEUE_INDEX_SQL = f"{TABLE_NAME}(check_create, id DESC)"  # 壊しにくい最小構成

# --- scripts（運用：env）---
SCRIPTS_DIR = _env_path("SCRIPTS_DIR", str(Path(__file__).resolve().parent)) or Path(__file__).resolve().parent

SCRIPT_02 = SCRIPTS_DIR / (_env_str("SCRIPT_02_NAME", "02_データ取得.py") or "02_データ取得.py")
SCRIPT_03 = SCRIPTS_DIR / (_env_str("SCRIPT_03_NAME", "03_画像生成.py") or "03_画像生成.py")
SCRIPT_04 = SCRIPTS_DIR / (_env_str("SCRIPT_04_NAME", "04_音声生成.py") or "04_音声生成.py")

# 05 は「サムネ/preview」スクリプト想定
SCRIPT_05 = SCRIPTS_DIR / (_env_str("SCRIPT_05_NAME", "05_サムネ動画作成.py") or "05_サムネ動画作成.py")

# 99 は「パーツ組み立て」想定
SCRIPT_99 = SCRIPTS_DIR / (_env_str("SCRIPT_99_NAME", "99_パーツ組み立て.py") or "99_パーツ組み立て.py")

# --- launcher behavior（運用：env）---
RUNS_DEFAULT = _env_int("RUNS_DEFAULT", 1)
STOP_ON_ERROR = _env_bool("STOP_ON_ERROR", False)
RESET_TO_ZERO_ON_FAIL_02 = _env_bool("RESET_TO_ZERO_ON_FAIL_02", False)
SLEEP_SEC_WHEN_EMPTY = float(_env_float("SLEEP_SEC_WHEN_EMPTY", 0.0))

# 05に folder_name を渡す互換運用（旧05を残す場合用）
PASS_FOLDER_NAME_TO_05 = _env_bool("PASS_FOLDER_NAME_TO_05", False)

# --- STA/END（運用：env）---
STA_02 = _env_int("STA_02", 1)
END_02 = _env_int("END_02", 2)

STA_03 = _env_int("STA_03", 2)
END_03 = _env_int("END_03", 3)

STA_04 = _env_int("STA_04", 3)
END_04 = _env_int("END_04", 4)

STA_05 = _env_int("STA_05", 4)
END_05 = _env_int("END_05", 5)

STA_99 = _env_int("STA_99", 5)
END_99 = _env_int("END_99", 6)

# --- タイムアウト（秒） 互換吸収 ---
TIMEOUT_02 = _env_opt_timeout("TIMEOUT_02", default=None)
TIMEOUT_03 = _env_opt_timeout("TIMEOUT_03", default=None)
TIMEOUT_04 = _env_opt_timeout("TIMEOUT_04", default=None)
# 旧 env は TIMEOUT_05_THUMB、現スクリプトは TIMEOUT_05 を見てたので両対応
TIMEOUT_05 = _env_opt_timeout("TIMEOUT_05_THUMB", "TIMEOUT_05", default=None)
# 旧/現どちらでも
TIMEOUT_99 = _env_opt_timeout("TIMEOUT_99", "TIMEOUT_98", default=None)

# --- sqlite pragmas（運用：env）---
BUSY_TIMEOUT_MS = _env_int("BUSY_TIMEOUT_MS", 60000)

# 互換：SQLITE_WAL=true があるなら WAL 優先。無ければ SQLITE_JOURNAL_MODE を使う。
SQLITE_WAL = _env_bool("SQLITE_WAL", True)
SQLITE_JOURNAL_MODE = _env_str("SQLITE_JOURNAL_MODE", "WAL" if SQLITE_WAL else "").strip()
SQLITE_SYNCHRONOUS = (_env_str("SQLITE_SYNCHRONOUS", "NORMAL") or "NORMAL").strip()


# =========================================================
# 共通
# =========================================================
def now_jst_str() -> str:
    return datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")


def banner(msg: str) -> None:
    print("\n" + "=" * 72)
    print(msg)
    print("=" * 72)


def step_line(n: int, total: int, title: str) -> None:
    print("\n" + "-" * 72)
    print(f"[STEP {n:02d}/{total:02d}] {title}")
    print("-" * 72)


def fmt_sec(sec: float) -> str:
    if sec < 60:
        return f"{sec:.1f}s"
    m = int(sec // 60)
    s = sec - (m * 60)
    return f"{m}m{s:.0f}s"


def connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path), timeout=BUSY_TIMEOUT_MS / 1000)
    con.row_factory = sqlite3.Row
    con.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS};")
    if SQLITE_JOURNAL_MODE:
        con.execute(f"PRAGMA journal_mode={SQLITE_JOURNAL_MODE};")
    if SQLITE_SYNCHRONOUS:
        con.execute(f"PRAGMA synchronous={SQLITE_SYNCHRONOUS};")
    return con


def ensure_columns(con: sqlite3.Connection) -> None:
    cur = con.execute(f"PRAGMA table_info({TABLE_NAME})")
    cols = {row[1] for row in cur.fetchall()}

    def add_col(sql: str):
        try:
            con.execute(sql)
        except Exception:
            pass

    if "check_create" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN check_create INTEGER DEFAULT 0")
    if "folder_name" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN folder_name TEXT")
    if "last_error" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN last_error TEXT")
    if "updated_at" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN updated_at TEXT")

    if "video_created" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_created INTEGER DEFAULT 0")
    if "video_created_at" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_created_at TEXT")
    if "video_uploaded" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_uploaded INTEGER DEFAULT 0")
    if "video_uploaded_at" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_uploaded_at TEXT")

    if ENABLE_PICK_QUEUE_INDEX:
        try:
            con.execute(
                f"CREATE INDEX IF NOT EXISTS {PICK_QUEUE_INDEX_NAME} ON {PICK_QUEUE_INDEX_SQL}"
            )
        except Exception:
            pass

    con.commit()


def update_item(con: sqlite3.Connection, item_id: int, *, check_create: int, last_error: Optional[str]) -> None:
    con.execute(
        f"""
        UPDATE {TABLE_NAME}
           SET check_create = ?,
               last_error   = ?,
               updated_at   = ?
         WHERE id = ?
        """,
        (int(check_create), last_error, now_jst_str(), int(item_id)),
    )
    con.commit()


def fetch_status(con: sqlite3.Connection, item_id: int) -> Tuple[int, str]:
    row = con.execute(
        f"SELECT check_create, COALESCE(folder_name,'') AS folder_name FROM {TABLE_NAME} WHERE id = ?",
        (int(item_id),),
    ).fetchone()
    if row is None:
        return (-999, "")
    return (int(row["check_create"]), str(row["folder_name"]))


def require_stage(con: sqlite3.Connection, item_id: int, expected: int, label: str) -> str:
    st, folder = fetch_status(con, item_id)
    print(f"[STATUS] {label}: check_create={st} folder_name={'(empty)' if not folder else folder}")
    if st != int(expected):
        raise RuntimeError(f"{label}: expected check_create={expected} but got {st} (id={item_id})")
    return folder


def run_script_realtime(script_path: Path, timeout: Optional[int], extra_args: Optional[List[str]] = None) -> None:
    if not script_path.exists():
        raise FileNotFoundError(f"script not found: {script_path}")

    cmd = [sys.executable, str(script_path)]
    if extra_args:
        cmd.extend(extra_args)

    print("[RUN]", " ".join(cmd))
    start = time.time()

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    try:
        assert p.stdout is not None
        for line in p.stdout:
            print(line.rstrip("\n"))
        rc = p.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        raise RuntimeError(f"{script_path.name} timeout")
    finally:
        elapsed = time.time() - start

    if rc != 0:
        raise RuntimeError(f"{script_path.name} failed (exit={rc})")

    print(f"[OK] {script_path.name} finished in {fmt_sec(elapsed)}")


def guard_unique_stage(con: sqlite3.Connection, stage: int) -> None:
    """
    stageが複数あると「別IDを拾う」事故が起きるので止める。
    各スクリプトは stage を見て自分で pick する前提なので必須ガード。
    """
    rows = con.execute(
        f"SELECT id FROM {TABLE_NAME} WHERE check_create=? ORDER BY id DESC LIMIT 50",
        (int(stage),),
    ).fetchall()
    if len(rows) != 1:
        ids = [str(r["id"]) for r in rows]
        raise RuntimeError(
            f"check_create={stage} が {len(rows)}件あります。"
            f"この状態だと次工程が別IDを拾う可能性があるので停止します。 ids={','.join(ids)}"
        )


def pick_inprogress_job(con: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """
    途中（STA群）を優先して拾う。
    数値の大小に依存しないよう、明示順（02→03→04→05→99）で並べる。
    """
    stages = (STA_02, STA_03, STA_04, STA_05, STA_99)
    q = ",".join(["?"] * len(stages))

    order_case = f"""
    CASE check_create
      WHEN {int(STA_02)} THEN 1
      WHEN {int(STA_03)} THEN 2
      WHEN {int(STA_04)} THEN 3
      WHEN {int(STA_05)} THEN 4
      WHEN {int(STA_99)} THEN 5
      ELSE 99
    END
    """

    return con.execute(
        f"""
        SELECT *
          FROM {TABLE_NAME}
         WHERE check_create IN ({q})
         ORDER BY {order_case} ASC, id DESC
         LIMIT 1
        """,
        tuple(int(x) for x in stages),
    ).fetchone()


def lock_new_job_atomic(con: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """新規(0)を拾って、0→STA_02 を原子的に実行。"""
    con.execute("BEGIN IMMEDIATE;")
    try:
        row = con.execute(
            f"""
            SELECT *
              FROM {TABLE_NAME}
             WHERE check_create = 0
             ORDER BY {PICK_NEW_ORDER_SQL}
             LIMIT 1
            """
        ).fetchone()

        if not row:
            con.execute("ROLLBACK;")
            return None

        item_id = int(row["id"])

        con.execute(
            f"""
            UPDATE {TABLE_NAME}
               SET check_create = ?,
                   last_error   = NULL,
                   updated_at   = ?
             WHERE id = ? AND check_create = 0
            """,
            (int(STA_02), now_jst_str(), int(item_id)),
        )

        if con.total_changes == 0:
            con.execute("ROLLBACK;")
            return None

        con.execute("COMMIT;")
        return con.execute(f"SELECT * FROM {TABLE_NAME} WHERE id=?", (int(item_id),)).fetchone()

    except Exception:
        con.execute("ROLLBACK;")
        raise


def process_one_item(con: sqlite3.Connection) -> int:
    ensure_columns(con)

    row = pick_inprogress_job(con)
    if row is None:
        row = lock_new_job_atomic(con)

    if row is None:
        print("[INFO] no item to process (no 0 and no STA stages).")
        return 0

    item_id = int(row["id"])
    st = int(row["check_create"])
    print(f"[INFO] picked id={item_id} (check_create={st})")

    total_steps = 5  # 02/03/04/05/99

    # ---- 02 ----
    if st == STA_02:
        step_line(1, total_steps, "02_データ取得.py START")
        try:
            guard_unique_stage(con, STA_02)
            require_stage(con, item_id, expected=STA_02, label="before 02")
            run_script_realtime(SCRIPT_02, TIMEOUT_02)
            folder_name = require_stage(con, item_id, expected=END_02, label="after 02")
            if not folder_name.strip():
                raise RuntimeError("after 02: folder_name is empty")
            st = END_02
        except Exception as e:
            err = f"02 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            if RESET_TO_ZERO_ON_FAIL_02:
                update_item(con, item_id, check_create=0, last_error=err)
                print(f"[INFO] reset id={item_id} to check_create=0")
            else:
                update_item(con, item_id, check_create=STA_02, last_error=err)
                print(f"[INFO] kept id={item_id} at check_create={STA_02}")
            return 1

    # ---- 03 ----
    if st == STA_03:
        step_line(2, total_steps, "03_画像生成.py START")
        try:
            guard_unique_stage(con, STA_03)
            require_stage(con, item_id, expected=STA_03, label="before 03")
            run_script_realtime(SCRIPT_03, TIMEOUT_03)
            require_stage(con, item_id, expected=END_03, label="after 03")
            st = END_03
        except Exception as e:
            err = f"03 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            update_item(con, item_id, check_create=STA_03, last_error=err)
            print(f"[INFO] kept id={item_id} at check_create={STA_03} (retry 03)")
            return 1

    # ---- 04 ----
    if st == STA_04:
        step_line(3, total_steps, "04_音声生成.py START")
        try:
            guard_unique_stage(con, STA_04)
            require_stage(con, item_id, expected=STA_04, label="before 04")
            run_script_realtime(SCRIPT_04, TIMEOUT_04)
            require_stage(con, item_id, expected=END_04, label="after 04")
            st = END_04
        except Exception as e:
            err = f"04 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            update_item(con, item_id, check_create=STA_04, last_error=err)
            print(f"[INFO] kept id={item_id} at check_create={STA_04} (retry 04)")
            return 1

    # ---- 05 ----
    if st == STA_05:
        step_line(4, total_steps, "05_サムネ/preview 作成 START")
        try:
            guard_unique_stage(con, STA_05)
            folder_name = require_stage(con, item_id, expected=STA_05, label="before 05")

            extra = None
            if PASS_FOLDER_NAME_TO_05:
                extra = ["--folder_name", folder_name]

            run_script_realtime(SCRIPT_05, TIMEOUT_05, extra_args=extra)

            require_stage(con, item_id, expected=END_05, label="after 05")
            st = END_05
        except Exception as e:
            err = f"05 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            update_item(con, item_id, check_create=STA_05, last_error=err)
            print(f"[INFO] kept id={item_id} at check_create={STA_05} (retry 05)")
            return 1

    # ---- 99 ----
    if st == STA_99:
        step_line(5, total_steps, "99_パーツ組み立て.py START")
        try:
            guard_unique_stage(con, STA_99)
            require_stage(con, item_id, expected=STA_99, label="before 99")
            run_script_realtime(SCRIPT_99, TIMEOUT_99)
            require_stage(con, item_id, expected=END_99, label="after 99")
            banner(f"[DONE] pipeline finished id={item_id}  {now_jst_str()}")
            return 0
        except Exception as e:
            err = f"99 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            update_item(con, item_id, check_create=STA_99, last_error=err)
            print(f"[INFO] kept id={item_id} at check_create={STA_99} (retry 99)")
            return 1

    print(f"[INFO] id={item_id} stage={st} is not a STA stage (skip).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=RUNS_DEFAULT, help="パイプラインを回す回数（RUNS_DEFAULTがデフォ）")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print(f"[ERROR] DB not found: {DB_PATH}", file=sys.stderr)
        return 2

    banner(f"LAUNCHER START  {now_jst_str()}  runs={args.runs}")
    print(f"[CONF] env              : {_ENV_PATH}")
    print(f"[CONF] DB_PATH          : {DB_PATH}")
    print(f"[CONF] TABLE_NAME       : {TABLE_NAME}")
    print(f"[CONF] SCRIPTS_DIR       : {SCRIPTS_DIR}")
    print(f"[CONF] PICK_NEW_ORDER_SQL: {PICK_NEW_ORDER_SQL} (code)")
    print(f"[CONF] ENABLE_PICK_QUEUE_INDEX : {ENABLE_PICK_QUEUE_INDEX}")
    print(f"[CONF] PICK_QUEUE_INDEX_NAME   : {PICK_QUEUE_INDEX_NAME}")
    print(f"[CONF] STOP_ON_ERROR     : {STOP_ON_ERROR}")
    print(f"[CONF] RESET_TO_ZERO_ON_FAIL_02: {RESET_TO_ZERO_ON_FAIL_02}")
    print(f"[CONF] SLEEP_SEC_WHEN_EMPTY: {SLEEP_SEC_WHEN_EMPTY}")
    print(f"[CONF] sqlite journal_mode={SQLITE_JOURNAL_MODE} synchronous={SQLITE_SYNCHRONOUS} busy_timeout_ms={BUSY_TIMEOUT_MS}")

    print(f"[CONF] STA/END 02: {STA_02}->{END_02}")
    print(f"[CONF] STA/END 03: {STA_03}->{END_03}")
    print(f"[CONF] STA/END 04: {STA_04}->{END_04}")
    print(f"[CONF] STA/END 05: {STA_05}->{END_05}")
    print(f"[CONF] STA/END 99: {STA_99}->{END_99}")
    print(f"[CONF] PASS_FOLDER_NAME_TO_05: {PASS_FOLDER_NAME_TO_05}")

    print(f"[CONF] TIMEOUT_02 : {TIMEOUT_02}")
    print(f"[CONF] TIMEOUT_03 : {TIMEOUT_03}")
    print(f"[CONF] TIMEOUT_04 : {TIMEOUT_04}")
    print(f"[CONF] TIMEOUT_05 : {TIMEOUT_05}")
    print(f"[CONF] TIMEOUT_99 : {TIMEOUT_99}")

    # 事前に存在チェック（早期に気づける）
    for p in (SCRIPT_02, SCRIPT_03, SCRIPT_04, SCRIPT_05, SCRIPT_99):
        if not p.exists():
            print(f"[WARN] script not found at startup: {p}", file=sys.stderr)

    ok_count = 0
    err_count = 0

    with connect(DB_PATH) as con:
        ensure_columns(con)

        for i in range(args.runs):
            print(f"\n[LOOP] {i+1}/{args.runs}")
            rc = process_one_item(con)

            if rc == 0:
                ok_count += 1
            else:
                err_count += 1
                if STOP_ON_ERROR:
                    print("[INFO] STOP_ON_ERROR=True -> stop.")
                    break

            if SLEEP_SEC_WHEN_EMPTY > 0:
                time.sleep(SLEEP_SEC_WHEN_EMPTY)

    banner(f"LAUNCHER END  {now_jst_str()}  ok={ok_count} err={err_count}")
    return 0 if err_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
