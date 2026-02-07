#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import random
import re
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


# =============================================================================
# 設定（ここだけ変えればOK）
# =============================================================================
CFG = {
    # --- DB / paths ---
    "DB_PATH": r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/list_category_gossip.db",
    "TABLE_NAME": "items",
    "BASE_OUTPUT_ROOT": r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001",
    "API_DIR": r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/api",
#    "CLIENT_JSON_NAME": "client_secrets.json",  # .json無し指定でも自動補正
    "CLIENT_JSON_NAME": "client_secret_769746086148-u7ma3j71951biaee4anijl2prgr1n9l4.apps.googleusercontent.com.json",

    "TOKEN_NAME": "token.json",

    # --- pipeline statuses ---
    "READY_STAGE": 6,
    "UPLOADING_STAGE": 7,
    "DONE_STAGE": 8,
    "FAIL_BACK_STAGE": 6,

    # --- video file ---
    "MOVIE_SUBDIR": "movie",
    "VIDEO_GLOB": "*.mp4",
    "PREFERRED_MP4": "youtube_upload.mp4",

    # --- batch behavior ---
    "LIMIT": 1,                 # ★何本処理するか（最大）
    "SLEEP_BETWEEN_SEC": 0.0,    # ★各アップロード間の待機（秒）
    "STOP_ON_FIRST_ERROR": False,  # ★1本失敗したら止める（普段はFalse推奨）

    # --- scheduling (JST 기준で作ってUTCに変換) ---
    "SCHEDULE_ENABLED": True,    # ★予約公開を使うか（True推奨）
    "START_DELAY_MIN": 2,       # ★1本目は「今から何分後」に予約するか
    "INTERVAL_MIN": 10,           # ★2本目以降、何分刻みでずらすか（=5分刻み）
    "OFFSET_MODE": "by_index",   # "by_index"（idx*interval） / "fixed"（全て同じ時刻）
    "FORCE_RESCHEDULE": False,   # ★DBにpublish_atがあっても上書きするか
    "RESCHEDULE_IF_PAST": True,  # ★publish_at_utc が過去なら自動で未来に再設定するか
    "MIN_FUTURE_BUFFER_MIN": 10, # ★再設定するなら「最低でも今から何分後」にするか

    # --- youtube upload meta defaults ---
    "CATEGORY_ID": "22",
    "NOTIFY_SUBSCRIBERS": False,
    "MADE_FOR_KIDS": False,
    "DESCRIPTION_COMMON": "",

    # --- retry / robustness ---
    "RETRIABLE_STATUS_CODES": {500, 502, 503, 504},
    "MAX_RETRIES": 10,
    "BASE_SLEEP": 1.0,

    # --- logging ---
    "PRINT_UPLOAD_PROGRESS_EVERY_SEC": 2.0,
    "ERROR_STORE_CHARS": 12000,

    # --- oauth scopes ---
    "SCOPES": ["https://www.googleapis.com/auth/youtube.upload"],
}

# =============================================================================
# 内部定数
# =============================================================================
JST = ZoneInfo("Asia/Tokyo")
UTC = ZoneInfo("UTC")
TITLE_MAX_CHARS = 95
TAG_MAX_CHARS_EACH = 50
TAG_TOTAL_CHARS = 500


# =============================================================================
# ログ
# =============================================================================
def now_jst() -> datetime:
    return datetime.now(JST)


def now_jst_str() -> str:
    return now_jst().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_jst_str()}] {msg}")


def eprint(msg: str) -> None:
    print(f"[{now_jst_str()}] {msg}", file=sys.stderr)


def step(n: int, title: str) -> None:
    print("\n" + "-" * 72)
    print(f"[STEP {n:02d}] {title}")
    print("-" * 72)


# =============================================================================
# 文字整形
# =============================================================================
def clean_title(title: str) -> str:
    t = (title or "").strip()
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) > TITLE_MAX_CHARS:
        t = t[:TITLE_MAX_CHARS]
    return t


def normalize_tags(tags: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for t in tags:
        t2 = re.sub(r"\s+", " ", (t or "").strip())
        if not t2:
            continue
        if len(t2) > TAG_MAX_CHARS_EACH:
            t2 = t2[:TAG_MAX_CHARS_EACH]
        if t2 in seen:
            continue
        seen.add(t2)
        out.append(t2)

    total = 0
    capped: List[str] = []
    for t in out:
        if total + len(t) > TAG_TOTAL_CHARS:
            break
        capped.append(t)
        total += len(t)
    return capped


def parse_keywords_raw_to_tags(keywords_raw: Optional[str]) -> List[str]:
    if not keywords_raw:
        return []
    s = str(keywords_raw).strip()
    if not s:
        return []

    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                tags = [str(x).strip() for x in arr if str(x).strip()]
                return normalize_tags(tags)
        except Exception:
            pass

    parts = re.split(r"[,\n\r\t　 /|]+", s)
    tags = [p.strip() for p in parts if p.strip()]
    return normalize_tags(tags)


# =============================================================================
# client_secret .json 付け忘れ吸収
# =============================================================================
def resolve_client_secrets_path(p: Path) -> Path:
    if p.exists():
        return p
    p2 = Path(str(p) + ".json")
    if p2.exists():
        eprint(f"[WARN] client secrets missing .json? using: {p2}")
        return p2
    raise FileNotFoundError(f"client secrets not found: {p} (also tried {p2})")


# =============================================================================
# 動画探索
# =============================================================================
def find_video_mp4(movie_dir: Path) -> Path:
    preferred = movie_dir / CFG["PREFERRED_MP4"]
    if preferred.exists():
        return preferred

    mp4s = sorted(movie_dir.glob(CFG["VIDEO_GLOB"]))
    if len(mp4s) == 0:
        raise FileNotFoundError(f"mp4が見つかりません: {movie_dir}")
    if len(mp4s) == 1:
        return mp4s[0]

    mp4s_sorted = sorted(mp4s, key=lambda p: p.stat().st_mtime, reverse=True)
    eprint(f"[WARN] mp4が複数あります。最新を使います: {[p.name for p in mp4s_sorted]}")
    return mp4s_sorted[0]


# =============================================================================
# DB
# =============================================================================
def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DBが見つかりません: {db_path}")
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


def ensure_columns(con: sqlite3.Connection, table_name: str) -> None:
    cur = con.execute(f"PRAGMA table_info({table_name})")
    cols = {row[1] for row in cur.fetchall()}

    must = {"id", "check_create", "folder_name", "post_title", "keywords_raw", "video_created", "video_uploaded"}
    missing = sorted(list(must - cols))
    if missing:
        raise RuntimeError(f"必須カラムが見つかりません: {missing}")

    add: List[Tuple[str, str]] = []
    # YouTube系（既にあれば何もしない）
    for name, ddl in [
        ("youtube_video_id", "TEXT"),
        ("youtube_uploaded_at", "TEXT"),
        ("youtube_status", "TEXT"),
        ("youtube_error", "TEXT"),
        ("publish_at_utc", "TEXT"),
        ("publish_at_jst", "TEXT"),
        ("published_at_utc", "TEXT"),
        ("video_created_at", "TEXT"),
        ("video_uploaded_at", "TEXT"),
    ]:
        if name not in cols:
            add.append((name, ddl))

    for name, ddl in add:
        try:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {name} {ddl}")
        except sqlite3.OperationalError:
            pass
    con.commit()


def set_yt_status(con: sqlite3.Connection, table_name: str, job_id: str, status: str, err: Optional[str] = None) -> None:
    try:
        con.execute(
            f"""
            UPDATE {table_name}
               SET youtube_status=?,
                   youtube_error=?
             WHERE id=?
            """,
            (status, None if err is None else err[:CFG["ERROR_STORE_CHARS"]], job_id),
        )
        con.commit()
    except sqlite3.OperationalError:
        pass


@dataclass
class JobRow:
    id: str
    folder_name: str
    post_title: str
    keywords_raw: Optional[str]
    check_create: int
    video_created: int
    video_uploaded: int
    publish_at_utc: str
    publish_at_jst: str


def fetch_upload_queue(con: sqlite3.Connection, table_name: str, limit: int) -> List[JobRow]:
    rows = con.execute(
        f"""
        SELECT id,
               COALESCE(folder_name,'') AS folder_name,
               COALESCE(post_title,'')  AS post_title,
               keywords_raw,
               check_create,
               COALESCE(video_created,0) AS video_created,
               COALESCE(video_uploaded,0) AS video_uploaded,
               COALESCE(publish_at_utc,'') AS publish_at_utc,
               COALESCE(publish_at_jst,'') AS publish_at_jst
          FROM {table_name}
         WHERE check_create = ?
           AND COALESCE(video_created,0) = 1
           AND COALESCE(video_uploaded,0) = 0
           AND COALESCE(folder_name,'') != ''
         ORDER BY CAST(id AS INTEGER) ASC
         LIMIT ?
        """,
        (CFG["READY_STAGE"], limit),
    ).fetchall()

    out: List[JobRow] = []
    for r in rows:
        out.append(
            JobRow(
                id=str(r["id"]),
                folder_name=str(r["folder_name"]),
                post_title=str(r["post_title"]),
                keywords_raw=None if r["keywords_raw"] is None else str(r["keywords_raw"]),
                check_create=int(r["check_create"]),
                video_created=int(r["video_created"]),
                video_uploaded=int(r["video_uploaded"]),
                publish_at_utc=str(r["publish_at_utc"]),
                publish_at_jst=str(r["publish_at_jst"]),
            )
        )
    return out


def lock_job_5_to_6(con: sqlite3.Connection, table_name: str, job_id: str) -> None:
    con.execute("BEGIN IMMEDIATE;")
    row = con.execute(
        f"SELECT check_create, COALESCE(video_uploaded,0) AS video_uploaded FROM {table_name} WHERE id=?",
        (job_id,),
    ).fetchone()
    if not row:
        con.execute("ROLLBACK;")
        raise RuntimeError(f"id not found: {job_id}")

    st = int(row["check_create"])
    vu = int(row["video_uploaded"])
    if vu == 1:
        con.execute("ROLLBACK;")
        raise RuntimeError(f"already uploaded: id={job_id}")
    if st != CFG["READY_STAGE"]:
        con.execute("ROLLBACK;")
        raise RuntimeError(f"expected check_create={CFG['READY_STAGE']} but got {st}: id={job_id}")

    cur = con.execute(
        f"""
        UPDATE {table_name}
           SET check_create=?,
               youtube_status=?,
               youtube_error=?
         WHERE id=? AND check_create=? AND COALESCE(video_uploaded,0)=0
        """,
        (CFG["UPLOADING_STAGE"], "lock_to_6", None, job_id, CFG["READY_STAGE"]),
    )
    if cur.rowcount != 1:
        con.execute("ROLLBACK;")
        raise RuntimeError(f"lock failed (rowcount!=1): id={job_id}")
    con.execute("COMMIT;")


def set_publish_at(con: sqlite3.Connection, table_name: str, job_id: str, publish_at_utc: str, publish_at_jst: str) -> None:
    con.execute(
        f"""
        UPDATE {table_name}
           SET publish_at_utc=?,
               publish_at_jst=?,
               youtube_status=?
         WHERE id=?
        """,
        (publish_at_utc, publish_at_jst, f"scheduled:{publish_at_utc}", job_id),
    )
    con.commit()


def mark_done(con: sqlite3.Connection, table_name: str, job_id: str, video_id: str) -> None:
    con.execute(
        f"""
        UPDATE {table_name}
           SET check_create=?,
               video_uploaded=1,
               video_uploaded_at=?,
               youtube_video_id=?,
               youtube_uploaded_at=?,
               youtube_status=?,
               youtube_error=?
         WHERE id=?
        """,
        (CFG["DONE_STAGE"], now_jst_str(), video_id, now_jst_str(), "uploaded", None, job_id),
    )
    con.commit()


def mark_fail_back(con: sqlite3.Connection, table_name: str, job_id: str, err: str) -> None:
    con.execute(
        f"""
        UPDATE {table_name}
           SET check_create=?,
               youtube_status=?,
               youtube_error=?
         WHERE id=?
        """,
        (CFG["FAIL_BACK_STAGE"], "failed", err[:CFG["ERROR_STORE_CHARS"]], job_id),
    )
    con.commit()


# =============================================================================
# OAuth / YouTube
# =============================================================================
def http_error_to_text(e: HttpError) -> str:
    code = getattr(e.resp, "status", None)
    content = getattr(e, "content", b"")
    if isinstance(content, bytes):
        content_text = content.decode("utf-8", errors="replace")
    else:
        content_text = str(content)
    return f"HttpError status={code} content={content_text}"


def get_authenticated_service(client_secrets_file: Path, token_file: Path) -> Any:
    client_secrets_file = resolve_client_secrets_path(client_secrets_file)

    creds: Optional[Credentials] = None
    if token_file.exists():
        creds = Credentials.from_authorized_user_file(str(token_file), CFG["SCOPES"])

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log("[AUTH] refreshing token...")
            creds.refresh(Request())
        else:
            log("[AUTH] starting browser OAuth flow...")
            flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets_file), CFG["SCOPES"])
            creds = flow.run_local_server(port=0, open_browser=True)

        token_file.parent.mkdir(parents=True, exist_ok=True)
        token_file.write_text(creds.to_json(), encoding="utf-8")

    return build("youtube", "v3", credentials=creds)


def fmt_bytes(n: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024.0:
            return f"{x:.1f}{u}"
        x /= 1024.0
    return f"{x:.1f}PB"


def upload_video(
    youtube: Any,
    video_path: Path,
    title: str,
    description: str,
    tags: List[str],
    category_id: str,
    privacy_status: str,
    notify_subscribers: bool,
    made_for_kids: bool,
    publish_at_utc: Optional[str],
    on_progress: Optional[Callable[[int], None]] = None,
) -> str:
    size = video_path.stat().st_size
    if size <= 0:
        raise ValueError(f"動画ファイルが空です: {video_path}")

    body: dict = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": category_id,
        },
        "status": {
            "privacyStatus": privacy_status,
            "selfDeclaredMadeForKids": bool(made_for_kids),
        },
    }
    if publish_at_utc:
        body["status"]["publishAt"] = publish_at_utc

    media = MediaFileUpload(str(video_path), mimetype="video/*", resumable=True, chunksize=1024 * 1024 * 8)
    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
        notifySubscribers=bool(notify_subscribers),
    )

    log(f"[UPLOAD] {video_path.name} ({fmt_bytes(size)})")
    log(f"[META] title={title}")
    log(f"[META] tags_count={len(tags)} privacy={privacy_status} publishAt={publish_at_utc or '(none)'}")

    response = None
    retry = 0

    last_print_t = 0.0
    last_bytes = 0.0
    last_t = time.time()

    while response is None:
        try:
            status, response = request.next_chunk()

            if status:
                p = float(status.progress())
                uploaded = p * size
                now = time.time()

                if now - last_print_t >= CFG["PRINT_UPLOAD_PROGRESS_EVERY_SEC"]:
                    dt = max(1e-6, now - last_t)
                    db = max(0.0, uploaded - last_bytes)
                    speed = db / dt
                    eta = (size - uploaded) / speed if speed > 0 else -1

                    pct = int(p * 100)
                    if eta >= 0:
                        log(f"[PROGRESS] {pct:3d}%  {fmt_bytes(uploaded)}/{fmt_bytes(size)}  {fmt_bytes(speed)}/s  ETA {eta:.0f}s")
                    else:
                        log(f"[PROGRESS] {pct:3d}%  {fmt_bytes(uploaded)}/{fmt_bytes(size)}")

                    if on_progress:
                        on_progress(pct)

                    last_print_t = now
                    last_bytes = uploaded
                    last_t = now

            if response and "id" in response:
                vid = str(response["id"])
                log(f"[DONE] videoId={vid}")
                return vid

        except HttpError as e:
            code = getattr(e.resp, "status", None)
            if code in CFG["RETRIABLE_STATUS_CODES"] and retry < CFG["MAX_RETRIES"]:
                retry += 1
                sleep = CFG["BASE_SLEEP"] * (2 ** (retry - 1)) + random.random()
                eprint(f"[WARN] HttpError {code}. retry={retry}/{CFG['MAX_RETRIES']} sleep={sleep:.1f}s")
                time.sleep(sleep)
                continue
            raise RuntimeError(http_error_to_text(e))

        except Exception as e:
            if retry < CFG["MAX_RETRIES"]:
                retry += 1
                sleep = CFG["BASE_SLEEP"] * (2 ** (retry - 1)) + random.random()
                eprint(f"[WARN] {type(e).__name__}: {e}. retry={retry}/{CFG['MAX_RETRIES']} sleep={sleep:.1f}s")
                time.sleep(sleep)
                continue
            raise

    raise RuntimeError("Upload failed without response.")


# =============================================================================
# publishAt 計算
# =============================================================================
def to_rfc3339_utc(dt_utc: datetime) -> str:
    return dt_utc.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_jst_human(dt_jst: datetime) -> str:
    return dt_jst.astimezone(JST).strftime("%Y-%m-%d %H:%M:%S")


def is_rfc3339_past(publish_at_utc: str, now_utc: datetime) -> bool:
    s = (publish_at_utc or "").strip()
    if not s:
        return False
    try:
        # "YYYY-MM-DDTHH:MM:SSZ"
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        return dt <= now_utc
    except Exception:
        # 形式が違う場合は安全側で「過去判定しない」
        return False


def compute_publish_time(base_now_jst: datetime, idx0: int) -> Tuple[str, str]:
    if not CFG["SCHEDULE_ENABLED"]:
        return ("", "")

    start_delay = int(CFG["START_DELAY_MIN"])
    interval = int(CFG["INTERVAL_MIN"])
    mode = str(CFG["OFFSET_MODE"])

    offset = start_delay
    if mode == "by_index":
        offset = start_delay + (interval * idx0)
    elif mode == "fixed":
        offset = start_delay

    dt_jst = base_now_jst + timedelta(minutes=offset)
    dt_utc = dt_jst.astimezone(UTC)
    return to_rfc3339_utc(dt_utc), to_jst_human(dt_jst)


# =============================================================================
# main
# =============================================================================
def main() -> int:
    ap = argparse.ArgumentParser()
    # 便利：CLI指定があればCFGを上書き（普段はCFGだけ編集でOK）
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--start_delay_min", type=int, default=None)
    ap.add_argument("--interval_min", type=int, default=None)
    ap.add_argument("--sleep_between_sec", type=float, default=None)
    ap.add_argument("--force_reschedule", action="store_true")
    ap.add_argument("--no_schedule", action="store_true")
    args = ap.parse_args()

    if args.limit is not None:
        CFG["LIMIT"] = int(args.limit)
    if args.start_delay_min is not None:
        CFG["START_DELAY_MIN"] = int(args.start_delay_min)
    if args.interval_min is not None:
        CFG["INTERVAL_MIN"] = int(args.interval_min)
    if args.sleep_between_sec is not None:
        CFG["SLEEP_BETWEEN_SEC"] = float(args.sleep_between_sec)
    if args.force_reschedule:
        CFG["FORCE_RESCHEDULE"] = True
    if args.no_schedule:
        CFG["SCHEDULE_ENABLED"] = False

    db_path = Path(CFG["DB_PATH"])
    table_name = str(CFG["TABLE_NAME"])
    base_root = Path(CFG["BASE_OUTPUT_ROOT"])

    api_dir = Path(CFG["API_DIR"])
    client_secrets = api_dir / CFG["CLIENT_JSON_NAME"]
    token_file = api_dir / CFG["TOKEN_NAME"]

    print("========================================================================")
    print(f"06_BATCH_UPLOAD START  {now_jst_str()}")
    print("========================================================================")
    print(f"[CONF] DB     : {db_path}")
    print(f"[CONF] TABLE  : {table_name}")
    print(f"[CONF] BASE   : {base_root}")
    print(f"[CONF] CLIENT : {client_secrets}")
    print(f"[CONF] TOKEN  : {token_file}")
    print(f"[CONF] LIMIT  : {CFG['LIMIT']}")
    print(f"[CONF] SCHEDULE_ENABLED : {CFG['SCHEDULE_ENABLED']}")
    print(f"[CONF] START_DELAY_MIN  : {CFG['START_DELAY_MIN']}")
    print(f"[CONF] INTERVAL_MIN     : {CFG['INTERVAL_MIN']}")
    print(f"[CONF] OFFSET_MODE      : {CFG['OFFSET_MODE']}")
    print(f"[CONF] FORCE_RESCHEDULE : {CFG['FORCE_RESCHEDULE']}")
    print(f"[CONF] RESCHEDULE_IF_PAST: {CFG['RESCHEDULE_IF_PAST']}")
    print(f"[CONF] SLEEP_BETWEEN_SEC: {CFG['SLEEP_BETWEEN_SEC']}")
    print("========================================================================")

    con = connect_db(db_path)
    try:
        step(1, "ensure_columns")
        ensure_columns(con, table_name)

        step(2, "build youtube service (OAuth)")
        yt = get_authenticated_service(client_secrets, token_file)

        step(3, "fetch upload queue")
        queue = fetch_upload_queue(con, table_name, limit=max(0, int(CFG["LIMIT"])))
        log(f"[QUEUE] found={len(queue)}")

        if not queue:
            log("[INFO] queue is empty. (check_create=5 & video_created=1 & video_uploaded=0)")
            return 0

        base_now = now_jst()
        now_utc = datetime.now(UTC)

        ok = 0
        ng = 0

        for i, job in enumerate(queue):
            log("")
            log(f"[ITEM] {i+1}/{len(queue)} id={job.id} folder={job.folder_name}")

            try:
                # ロック（5->6）
                set_yt_status(con, table_name, job.id, "step:lock_start")
                lock_job_5_to_6(con, table_name, job.id)
                set_yt_status(con, table_name, job.id, "step:locked_to_6")

                # publishAt 決定
                publish_at_utc = job.publish_at_utc.strip()
                publish_at_jst = job.publish_at_jst.strip()

                need_set = False
                if CFG["SCHEDULE_ENABLED"]:
                    if CFG["FORCE_RESCHEDULE"]:
                        need_set = True
                    elif not publish_at_utc:
                        need_set = True
                    elif CFG["RESCHEDULE_IF_PAST"] and is_rfc3339_past(publish_at_utc, now_utc):
                        # 過去なら未来に再設定
                        need_set = True

                    if need_set:
                        pub_utc, pub_jst = compute_publish_time(base_now, i)

                        # 最低未来バッファ（安全策）
                        min_future = now_jst() + timedelta(minutes=int(CFG["MIN_FUTURE_BUFFER_MIN"]))
                        min_utc = to_rfc3339_utc(min_future.astimezone(UTC))
                        # pub_utcがmin_utcより前なら、min_future側に寄せる
                        try:
                            dt_pub = datetime.strptime(pub_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
                            dt_min = datetime.strptime(min_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
                            if dt_pub < dt_min:
                                pub_utc = min_utc
                                pub_jst = to_jst_human(min_future)
                        except Exception:
                            pass

                        publish_at_utc, publish_at_jst = pub_utc, pub_jst
                        set_publish_at(con, table_name, job.id, publish_at_utc, publish_at_jst)
                        log(f"[SCHEDULE] set publish_at_jst={publish_at_jst} publish_at_utc={publish_at_utc}")
                    else:
                        log(f"[SCHEDULE] keep publish_at_jst={publish_at_jst} publish_at_utc={publish_at_utc}")
                        set_yt_status(con, table_name, job.id, f"scheduled:{publish_at_utc}")
                else:
                    publish_at_utc = ""
                    publish_at_jst = ""
                    log("[SCHEDULE] disabled")

                # パス解決
                parent_dir = base_root / job.folder_name
                movie_dir = parent_dir / CFG["MOVIE_SUBDIR"]
                video_path = find_video_mp4(movie_dir)

                log(f"[PATH] video={video_path}")
                set_yt_status(con, table_name, job.id, f"step:video_found:{video_path.name}")

                # メタ
                title = clean_title(job.post_title) or clean_title(video_path.stem)
                tags = parse_keywords_raw_to_tags(job.keywords_raw)

                description = (CFG["DESCRIPTION_COMMON"] or "").strip()
                if tags:
                    tags_line = " ".join([f"#{t.replace(' ', '')}" for t in tags[:20]])
                    description = (description + "\n\n" + tags_line).strip() if description else tags_line

                # 予約公開なら privacy=private が必須
                privacy = "private" if (CFG["SCHEDULE_ENABLED"] and publish_at_utc) else "private"

                set_yt_status(con, table_name, job.id, "step:upload_start")

                def on_prog(pct: int) -> None:
                    if pct % 10 == 0:
                        set_yt_status(con, table_name, job.id, f"uploading:{pct}%")

                video_id = upload_video(
                    youtube=yt,
                    video_path=video_path,
                    title=title,
                    description=description,
                    tags=tags,
                    category_id=str(CFG["CATEGORY_ID"]),
                    privacy_status=privacy,
                    notify_subscribers=bool(CFG["NOTIFY_SUBSCRIBERS"]),
                    made_for_kids=bool(CFG["MADE_FOR_KIDS"]),
                    publish_at_utc=(publish_at_utc if (CFG["SCHEDULE_ENABLED"] and publish_at_utc) else None),
                    on_progress=on_prog,
                )

                mark_done(con, table_name, job.id, video_id)
                log(f"[OK] uploaded id={job.id} videoId={video_id} publish_at_jst={publish_at_jst}")
                ok += 1

                if float(CFG["SLEEP_BETWEEN_SEC"]) > 0:
                    time.sleep(float(CFG["SLEEP_BETWEEN_SEC"]))

            except Exception as e:
                tb = traceback.format_exc()
                eprint(f"[NG] id={job.id} {type(e).__name__}: {e}")
                eprint(tb)
                set_yt_status(con, table_name, job.id, "failed", tb)
                try:
                    mark_fail_back(con, table_name, job.id, tb)
                except Exception as e2:
                    eprint(f"[WARN] failed to write fail-back to DB: {type(e2).__name__}: {e2}")
                ng += 1
                if CFG["STOP_ON_FIRST_ERROR"]:
                    eprint("[STOP] STOP_ON_FIRST_ERROR=True")
                    break

        step(4, "done")
        log(f"[SUMMARY] ok={ok} ng={ng} total={len(queue)}")
        return 0 if ng == 0 else 1

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
