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

# =========================================================
# 設定（ここだけ変えればOK）  ★全部ここに集約
# =========================================================

# --- DB ---
from config import DB_PATH
TABLE_NAME = "items"

# ★案1: 新規(0)の取得順（上から処理）
#   comments_count: 多い順（DESC）
#   post_date     : 古い順（ASC）
#   id            : 同点/同日時の安定化（DESC）
#PICK_NEW_ORDER_SQL = "comments_count DESC, post_date ASC, id DESC"
PICK_NEW_ORDER_SQL = "post_date DESC, id DESC"

# ★案1: 新規ピックを速くするインデックス（無ければ作る）
ENABLE_PICK_QUEUE_INDEX = True
PICK_QUEUE_INDEX_NAME = "idx_items_pick_queue"
PICK_QUEUE_INDEX_SQL = f"{TABLE_NAME}(check_create, comments_count DESC, post_date ASC)"

# --- scripts ---
SCRIPT_02 = Path(r"/Users/yumahama/Documents/Python/05＿ガールズチャンネル/01_暫定/02_データ取得.py")
SCRIPT_03 = Path(r"/Users/yumahama/Documents/Python/05＿ガールズチャンネル/01_暫定/03_画像生成.py")
SCRIPT_04 = Path(r"/Users/yumahama/Documents/Python/05＿ガールズチャンネル/01_暫定/04_音声生成.py")
SCRIPT_05 = Path(r"/Users/yumahama/Documents/Python/05＿ガールズチャンネル/01_暫定/05_パーツ組み立て.py")

# --- launcher behavior ---
RUNS_DEFAULT = 1 #実行回数

# タイムアウト（秒）※不要なら None
TIMEOUT_02 = None
TIMEOUT_03 = None
TIMEOUT_04 = None
TIMEOUT_05 = None

# 02が失敗したら 0 に戻す（再抽選したいなら True、同じIDに粘るなら False）
RESET_TO_ZERO_ON_FAIL_02 = False

# 失敗時に止めるか（Falseだと「次のループに進む」）
STOP_ON_ERROR = False

# 空になったときの待機（基本0でOK）
SLEEP_SEC_WHEN_EMPTY = 0

# --- sqlite pragmas ---
SQLITE_WAL = True
SQLITE_SYNCHRONOUS = "NORMAL"  # "OFF" / "NORMAL" / "FULL"

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
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    if SQLITE_WAL:
        con.execute("PRAGMA journal_mode=WAL;")
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

    # ★追加：動画作成/アップロードのフラグ
    if "video_created" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_created INTEGER DEFAULT 0")
    if "video_created_at" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_created_at TEXT")
    if "video_uploaded" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_uploaded INTEGER DEFAULT 0")
    if "video_uploaded_at" not in cols:
        add_col(f"ALTER TABLE {TABLE_NAME} ADD COLUMN video_uploaded_at TEXT")

    # ★案1用：ピック高速化インデックス（任意）
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
        (check_create, last_error, now_jst_str(), item_id),
    )
    con.commit()

def fetch_status(con: sqlite3.Connection, item_id: int) -> Tuple[int, str]:
    row = con.execute(
        f"SELECT check_create, COALESCE(folder_name,'') AS folder_name FROM {TABLE_NAME} WHERE id = ?",
        (item_id,),
    ).fetchone()
    if row is None:
        return (-999, "")
    return (int(row["check_create"]), str(row["folder_name"]))

def require_stage(con: sqlite3.Connection, item_id: int, expected: int, label: str) -> str:
    st, folder = fetch_status(con, item_id)
    print(f"[STATUS] {label}: check_create={st} folder_name={'(empty)' if not folder else folder}")
    if st != expected:
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

def pick_inprogress_job(con: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """
    途中（1-4）を優先して拾う。
    """
    return con.execute(
        f"""
        SELECT *
          FROM {TABLE_NAME}
         WHERE check_create IN (1,2,3,4)
         ORDER BY check_create ASC, id DESC
         LIMIT 1
        """
    ).fetchone()

def lock_new_job_atomic(con: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """
    ★案1:
    新規(0)を PICK_NEW_ORDER_SQL で上から拾って、0→1ロックを原子的に実行。
    """
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
               SET check_create = 1,
                   last_error   = NULL,
                   updated_at   = ?
             WHERE id = ? AND check_create = 0
            """,
            (now_jst_str(), item_id),
        )

        # 他で先にロックされた等で更新されなかった場合
        if con.total_changes == 0:
            con.execute("ROLLBACK;")
            return None

        con.execute("COMMIT;")

        return con.execute(f"SELECT * FROM {TABLE_NAME} WHERE id=?", (item_id,)).fetchone()

    except Exception:
        con.execute("ROLLBACK;")
        raise

def guard_unique_stage(con: sqlite3.Connection, stage: int) -> None:
    """
    02〜05は “idを渡さず statusで拾う” 作りなので、
    同じstageが複数あると別IDが拾われる危険がある。
    → stageが複数なら止めて、手動整理させる（安定運用のため）。
    """
    rows = con.execute(
        f"SELECT id FROM {TABLE_NAME} WHERE check_create=? ORDER BY id DESC LIMIT 50",
        (stage,),
    ).fetchall()
    if len(rows) != 1:
        ids = [str(r["id"]) for r in rows]
        raise RuntimeError(
            f"check_create={stage} が {len(rows)}件あります。"
            f"この状態だと 0{stage+1} が別IDを拾う可能性があるので停止します。 ids={','.join(ids)}"
        )

def process_one_item(con: sqlite3.Connection) -> int:
    ensure_columns(con)

    # まず途中(1-4)を優先
    row = pick_inprogress_job(con)

    if row is None:
        # 途中が無ければ、新規(0)を案1順で拾って原子的に0→1ロック
        row = lock_new_job_atomic(con)

    if row is None:
        print("[INFO] no item to process (no 0 and no 1-4).")
        return 0

    item_id = int(row["id"])
    st = int(row["check_create"])
    print(f"[INFO] picked id={item_id} (check_create={st})")

    total_steps = 4

    # ---- 02 ----
    if st <= 1:
        step_line(1, total_steps, "02_データ取得.py START")
        try:
            guard_unique_stage(con, 1)
            require_stage(con, item_id, expected=1, label="before 02")
            run_script_realtime(SCRIPT_02, TIMEOUT_02)
            folder_name = require_stage(con, item_id, expected=2, label="after 02")
            if not folder_name.strip():
                raise RuntimeError("after 02: folder_name is empty")
            st = 2
        except Exception as e:
            err = f"02 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            if RESET_TO_ZERO_ON_FAIL_02:
                update_item(con, item_id, check_create=0, last_error=err)
                print(f"[INFO] reset id={item_id} to check_create=0")
            else:
                update_item(con, item_id, check_create=1, last_error=err)
                print(f"[INFO] kept id={item_id} at check_create=1")
            return 1

    # ---- 03 ----
    if st <= 2:
        step_line(2, total_steps, "03_画像生成.py START")
        try:
            guard_unique_stage(con, 2)
            require_stage(con, item_id, expected=2, label="before 03")
            run_script_realtime(SCRIPT_03, TIMEOUT_03)
            require_stage(con, item_id, expected=3, label="after 03")
            st = 3
        except Exception as e:
            err = f"03 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            update_item(con, item_id, check_create=2, last_error=err)
            print(f"[INFO] set id={item_id} back to check_create=2 (retry 03)")
            return 1

    # ---- 04 ----
    if st <= 3:
        step_line(3, total_steps, "04_音声生成.py START")
        try:
            guard_unique_stage(con, 3)
            require_stage(con, item_id, expected=3, label="before 04")
            run_script_realtime(SCRIPT_04, TIMEOUT_04)
            require_stage(con, item_id, expected=4, label="after 04")
            st = 4
        except Exception as e:
            err = f"04 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            update_item(con, item_id, check_create=3, last_error=err)
            print(f"[INFO] set id={item_id} back to check_create=3 (retry 04)")
            return 1

    # ---- 05 ----
    if st <= 4:
        step_line(4, total_steps, "05_パーツ組み立て.py START")
        try:
            guard_unique_stage(con, 4)
            require_stage(con, item_id, expected=4, label="before 05")
            run_script_realtime(SCRIPT_05, TIMEOUT_05)
            require_stage(con, item_id, expected=5, label="after 05")
            banner(f"[DONE] video created id={item_id}  {now_jst_str()}")
            return 0
        except Exception as e:
            err = f"05 failed: {type(e).__name__}: {e}"
            print(f"[ERROR] {err}", file=sys.stderr)
            update_item(con, item_id, check_create=4, last_error=err)
            print(f"[INFO] set id={item_id} back to check_create=4 (retry 05)")
            return 1

    # st==5（すでに動画作成済み）
    print(f"[INFO] id={item_id} already check_create=5 (skip).")
    return 0

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=RUNS_DEFAULT, help="02〜05を回す回数")
    args = ap.parse_args()

    if not DB_PATH.exists():
        print(f"[ERROR] DB not found: {DB_PATH}", file=sys.stderr)
        return 2

    banner(f"LAUNCHER START  {now_jst_str()}  runs={args.runs}")
    print(f"[INFO] pick_new_rule: check_create=0 を ORDER BY {PICK_NEW_ORDER_SQL} で上から取得（案1）")

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
