#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
00_リスト作成.py（items_all / items_do / items_done 対応版 v6）

要件:
- items_all: id(INTEGER PK) / code(UNIQUE) / first_post / last_post / excluded など
- items_all: comment_average を追加
    comment_average = comments_count / minutes(last_post - first_post)
    ※ first_post 未取得 or diff<=0 は NULL
- items_do: id(INTEGER PK) / code(UNIQUE) / priority 追加（created_at/update_at無し）
    priority は items_all の (excluded=0, comment_average DESC, first_post DESC) で 1.. 採番して反映
- items_done: id(INTEGER PK) / code(UNIQUE)

注意:
- 既存DBが旧スキーマ（items_all.id が TEXT のスレッドID）でも自動移行:
    旧id(TEXT) → 新code(TEXT UNIQUE) にコピーし、items_all を作り直します。
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
import sqlite3
from zoneinfo import ZoneInfo

from tqdm import tqdm
from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


# =========================================================
# 設定（ここだけ変えればOK）
# =========================================================
BASE_DIR = Path("/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001")
DB_PATH = BASE_DIR / "00_db/list_girlsChannel_test.db"

TABLE_ALL  = "items_all"
TABLE_DO   = "items_do"
TABLE_DONE = "items_done"

PAGE_FROM = 1
PAGE_TO   = 15

TARGET_NEW_COUNT = 1000
MIN_COMMENTS = 400
UPDATE_EXISTING = True

HEADLESS = True
TIMEOUT_MS_LIST = 30000
TIMEOUT_MS_FIRSTPOST_GOTO = 12000
TIMEOUT_MS_FIRSTPOST_TEXT = 3000
SLEEP_SEC = 0.6
REQUEST_BLOCK = True
DETAIL_SLEEP_SEC = 0.05

ECHO_EACH_SAVE = True
EARLY_STOP_PAGES = 2

OUT_AUTO_WORDS = [
    "Part", "PART",
    "語ろう", "語りたい", "語りましょう",
    "アンチ厳禁", "ファントピ", "トピ",
    "結婚を発表", "妊娠",
    "ガルちゃん", "ｶﾞﾙ",
    "一周忌",
    "と思う芸能人","と思う有名人",

    "地震",
]

# 取得するカテゴリを0-basedで指定
ENABLED_CATEGORY_INDEXES: List[int] = [0, 1]


# =========================================================
# 正規表現
# =========================================================
RE_TOPIC_HREF = re.compile(r"/topics/(\d+)/")
RE_FIRSTPOST_ANY = re.compile(r"(\d{4})/(\d{2})/(\d{2}).*?(\d{2}):(\d{2}):(\d{2})")


# =========================================================
# 取得対象カテゴリ
# =========================================================
@dataclass(frozen=True)
class CategoryConfig:
    name: str
    base_url: str
    params: str


CATEGORIES: List[CategoryConfig] = [
    CategoryConfig("ゴシップ", "https://girlschannel.net/topics/category/gossip", "?sort=comment&date=m"),
    CategoryConfig("ニュース", "https://girlschannel.net/topics/category/news", "?sort=comment&date=m"),
    CategoryConfig("政治経済", "https://girlschannel.net/topics/category/politics", "?sort=comment&date=m"),
]


# =========================================================
# DDL
# =========================================================
DDL_ITEMS_ALL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_ALL} (
  id INTEGER PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,
  check_date TEXT NOT NULL,
  first_seen_at TEXT,
  first_post TEXT,
  last_post TEXT NOT NULL,
  comments_count INTEGER NOT NULL,
  category TEXT NOT NULL,
  title TEXT NOT NULL,
  out_auto INTEGER NOT NULL DEFAULT 0,
  out_manual INTEGER NOT NULL DEFAULT 0,
  excluded INTEGER NOT NULL DEFAULT 0,
  comment_average REAL,
  comment_average2 REAL
);


CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_code ON {TABLE_ALL}(code);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_check_date ON {TABLE_ALL}(check_date);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_first_seen_at ON {TABLE_ALL}(first_seen_at);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_first_post ON {TABLE_ALL}(first_post);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_last_post ON {TABLE_ALL}(last_post);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_comments ON {TABLE_ALL}(comments_count);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_category ON {TABLE_ALL}(category);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_out_auto ON {TABLE_ALL}(out_auto);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_out_manual ON {TABLE_ALL}(out_manual);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_excluded ON {TABLE_ALL}(excluded);
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_comment_average ON {TABLE_ALL}(comment_average);
"""

DDL_DO_DONE = f"""
CREATE TABLE IF NOT EXISTS {TABLE_DO} (
  id INTEGER PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'queued',
  step INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_{TABLE_DO}_priority ON {TABLE_DO}(priority);
CREATE INDEX IF NOT EXISTS idx_{TABLE_DO}_status ON {TABLE_DO}(status);

CREATE TABLE IF NOT EXISTS {TABLE_DONE} (
  id INTEGER PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL,
  done_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  youtube_status TEXT,
  youtube_video_id TEXT,
  youtube_url TEXT,
  tiktok_status TEXT,
  tiktok_url TEXT,
  notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_{TABLE_DONE}_done_at ON {TABLE_DONE}(done_at);
CREATE INDEX IF NOT EXISTS idx_{TABLE_DONE}_updated_at ON {TABLE_DONE}(updated_at);
"""


# =========================================================
# DB ユーティリティ
# =========================================================
def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path), timeout=30)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    ensure_schema(con)
    return con


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone() is not None


def _colnames(con: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table});").fetchall()]


def ensure_schema(con: sqlite3.Connection) -> None:
    """
    - items_all 旧スキーマ（id TEXT PK）なら code へ移行して作り直し
    - comment_average / priority の不足カラムは ALTER で追加
    - excluded 同期トリガー作成
    """
    if _table_exists(con, TABLE_ALL):
        cols = _colnames(con, TABLE_ALL)
        colset = set(cols)

        # 旧: code が無く、id が存在 → id がスレッドIDだった
        if "code" not in colset and "id" in colset:
            # point_out -> out_auto
            if "point_out" in colset and "out_auto" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} RENAME COLUMN point_out TO out_auto;")
                con.commit()
                colset = set(_colnames(con, TABLE_ALL))

            # post_date -> last_post
            if "post_date" in colset and "last_post" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} RENAME COLUMN post_date TO last_post;")
                con.commit()
                colset = set(_colnames(con, TABLE_ALL))

            # 足りない列を追加（移行前に揃える）
            if "first_seen_at" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN first_seen_at TEXT;")
            if "first_post" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN first_post TEXT;")
            if "out_auto" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN out_auto INTEGER NOT NULL DEFAULT 0;")
            if "out_manual" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN out_manual INTEGER NOT NULL DEFAULT 0;")
            if "excluded" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN excluded INTEGER NOT NULL DEFAULT 0;")
            con.commit()

            # first_seen_at が空なら check_date
            con.execute(
                f"""
                UPDATE {TABLE_ALL}
                   SET first_seen_at = check_date
                 WHERE (first_seen_at IS NULL OR first_seen_at = '')
                """
            )
            # excluded 再計算
            con.execute(
                f"""
                UPDATE {TABLE_ALL}
                   SET excluded = CASE WHEN COALESCE(out_auto,0)=1 OR COALESCE(out_manual,0)=1 THEN 1 ELSE 0 END
                """
            )
            con.commit()

            # 新スキーマへ移行
            tmp = f"{TABLE_ALL}__new"
            con.execute(f"DROP TABLE IF EXISTS {tmp};")
            con.commit()

            con.executescript(
                DDL_ITEMS_ALL.replace(
                    f"CREATE TABLE IF NOT EXISTS {TABLE_ALL}",
                    f"CREATE TABLE IF NOT EXISTS {tmp}",
                )
            )
            con.commit()

            con.execute(
                f"""
                INSERT INTO {tmp} (
                  code, check_date, first_seen_at, first_post, last_post,
                  comments_count, category, title, out_auto, out_manual, excluded, comment_average, comment_average2
                )
                SELECT
                  id AS code,
                  check_date, first_seen_at, first_post, last_post,
                  comments_count, category, title,
                  COALESCE(out_auto,0), COALESCE(out_manual,0), COALESCE(excluded,0),
                  NULL, NULL
                FROM {TABLE_ALL}
                """
            )
            con.commit()

            con.execute(f"DROP TABLE {TABLE_ALL};")
            con.execute(f"ALTER TABLE {tmp} RENAME TO {TABLE_ALL};")
            con.commit()

    # DDL 適用
    con.executescript(DDL_ITEMS_ALL)
    con.executescript(DDL_DO_DONE)
    con.commit()

    # 追加カラム補完（既存DB向け）
    cols_all = set(_colnames(con, TABLE_ALL))
    if "comment_average" not in cols_all:
        con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN comment_average REAL;")
        con.commit()

    cols_all = set(_colnames(con, TABLE_ALL))
    if "comment_average2" not in cols_all:
        con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN comment_average2 REAL;")
        con.commit()

    # comment_average2 用インデックス（既存DBで列追加後に作る）
    con.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_comment_average2 ON {TABLE_ALL}(comment_average2);")
    con.commit()

    cols_do = set(_colnames(con, TABLE_DO))
    if "priority" not in cols_do:
        con.execute(f"ALTER TABLE {TABLE_DO} ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;")
        con.commit()

    # excluded 同期トリガー
    con.execute("DROP TRIGGER IF EXISTS trg_items_all_excluded_sync_ai;")
    con.execute("DROP TRIGGER IF EXISTS trg_items_all_excluded_sync_au;")

    con.execute(
        f"""
        CREATE TRIGGER trg_items_all_excluded_sync_ai
        AFTER INSERT ON {TABLE_ALL}
        BEGIN
          UPDATE {TABLE_ALL}
             SET excluded = CASE WHEN NEW.out_auto=1 OR NEW.out_manual=1 THEN 1 ELSE 0 END
           WHERE rowid = NEW.rowid;
        END;
        """
    )
    con.execute(
        f"""
        CREATE TRIGGER trg_items_all_excluded_sync_au
        AFTER UPDATE OF out_auto, out_manual ON {TABLE_ALL}
        BEGIN
          UPDATE {TABLE_ALL}
             SET excluded = CASE WHEN NEW.out_auto=1 OR NEW.out_manual=1 THEN 1 ELSE 0 END
           WHERE rowid = NEW.rowid;
        END;
        """
    )
    con.commit()


def exists_code(con: sqlite3.Connection, code: str) -> bool:
    return con.execute(f"SELECT 1 FROM {TABLE_ALL} WHERE code=? LIMIT 1", (code,)).fetchone() is not None


def get_excluded(con: sqlite3.Connection, code: str) -> int:
    row = con.execute(f"SELECT excluded FROM {TABLE_ALL} WHERE code=? LIMIT 1", (code,)).fetchone()
    return int(row[0] or 0) if row else 0


def set_first_post_if_empty(con: sqlite3.Connection, code: str, first_post: str) -> None:
    con.execute(
        f"""
        UPDATE {TABLE_ALL}
           SET first_post = COALESCE(first_post, ?)
         WHERE code=?;
        """,
        (first_post, code),
    )


def upsert_items_all(con: sqlite3.Connection, row: Dict[str, Any]) -> None:
    sql = f"""
    INSERT INTO {TABLE_ALL} (
        code, check_date, first_seen_at, first_post, last_post,
        comments_count, category, title, out_auto
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(code) DO UPDATE SET
      check_date=excluded.check_date,
      first_seen_at=COALESCE({TABLE_ALL}.first_seen_at, excluded.first_seen_at),
      first_post=COALESCE({TABLE_ALL}.first_post, excluded.first_post),
      last_post=excluded.last_post,
      comments_count=excluded.comments_count,
      category=excluded.category,
      title=excluded.title,
      out_auto=excluded.out_auto
    """
    con.execute(
        sql,
        (
            row["code"],
            row["check_date"],
            row["first_seen_at"],
            row.get("first_post"),
            row["last_post"],
            int(row["comments_count"]),
            row["category"],
            row["title"],
            int(row["out_auto"]),
        ),
    )


def recompute_comment_average(con: sqlite3.Connection) -> None:
    """
    comment_average  = comments_count / minutes(last_post - first_post)  （小数点第2位）
    comment_average2 = comments_count / days(date(last_post) - date(first_post) + 1) （"日付差"のみ・小数点第2位）

    例:
      1/7 19:20 - 1/5 23:20 -> date差は 2日 なので days=2
    """
    con.execute(
        f"""
        UPDATE {TABLE_ALL}
           SET
             comment_average =
               CASE
                 WHEN first_post IS NULL OR first_post='' THEN NULL
                 WHEN last_post  IS NULL OR last_post ='' THEN NULL
                 ELSE
                   CASE
                     WHEN (julianday(last_post) - julianday(first_post)) * 24.0 * 60.0 <= 0 THEN NULL
                     ELSE ROUND((comments_count * 1.0) / ((julianday(last_post) - julianday(first_post)) * 24.0 * 60.0), 2)
                   END
               END,
             comment_average2 =
               CASE
                 WHEN first_post IS NULL OR first_post='' THEN NULL
                 WHEN last_post  IS NULL OR last_post ='' THEN NULL
                 ELSE
                   CASE
                     WHEN (julianday(date(last_post)) - julianday(date(first_post))) <= 0 THEN NULL
                     ELSE ROUND((comments_count * 1.0) / (julianday(date(last_post)) - julianday(date(first_post))), 2)
                   END
               END
        """
    )
    con.commit()


def sync_items_do_from_all(con: sqlite3.Connection) -> None:
    """
    items_all -> items_do 同期
    - excluded=0 を投入（titleのみ追随。status/step/エラー類は維持）
    - excluded=1 は削除
    - items_all から消えた code も削除
    - priority: (comment_average DESC, first_post DESC) で 1.. 採番して反映
    """
    # excluded=0 を追加/更新
    con.execute(
        f"""
        INSERT INTO {TABLE_DO} (code, title, status, step, priority)
        SELECT a.code, a.title, 'queued', 0, 0
          FROM {TABLE_ALL} a
         WHERE a.excluded = 0
        ON CONFLICT(code) DO UPDATE SET
          title=excluded.title
        """
    )
    upsert_changes = con.execute("SELECT changes();").fetchone()[0]

    # excluded=1 削除
    con.execute(
        f"""
        DELETE FROM {TABLE_DO}
         WHERE code IN (SELECT code FROM {TABLE_ALL} WHERE excluded=1)
        """
    )
    deleted_excluded = con.execute("SELECT changes();").fetchone()[0]

    # items_all から消えた code も削除
    con.execute(
        f"""
        DELETE FROM {TABLE_DO}
         WHERE code NOT IN (SELECT code FROM {TABLE_ALL})
        """
    )
    deleted_missing = con.execute("SELECT changes();").fetchone()[0]

    # priority 再計算（NULLは最後）
    con.execute(
        f"""
        WITH ranked AS (
          SELECT
            code,
            ROW_NUMBER() OVER (
              ORDER BY
                (comment_average IS NULL) ASC,
                comment_average DESC,
                (first_post IS NULL) ASC,
                first_post DESC
            ) AS pr
          FROM {TABLE_ALL}
          WHERE excluded = 0
        )
        UPDATE {TABLE_DO}
           SET priority = (SELECT pr FROM ranked WHERE ranked.code = {TABLE_DO}.code)
         WHERE code IN (SELECT code FROM ranked)
        """
    )
    pr_updated = con.execute("SELECT changes();").fetchone()[0]

    con.commit()

    total_do = con.execute(f"SELECT COUNT(*) FROM {TABLE_DO};").fetchone()[0]
    print(
        f"[SYNC] {TABLE_DO}: upsert_changes={upsert_changes} "
        f"deleted_excluded={deleted_excluded} deleted_missing={deleted_missing} "
        f"priority_updated={pr_updated} total={total_do}"
    )


# =========================================================
# 変換/判定
# =========================================================
def digits_only_int(s: str) -> int:
    nums = re.findall(r"\d+", (s or "").replace(",", ""))
    return int("".join(nums)) if nums else 0


def normalize_list_datetime(raw: str) -> str:
    txt = (raw or "").strip()
    if not txt:
        return "1970-01-01 00:00:00"
    try:
        dt = dtparser.parse(txt, fuzzy=True)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return txt


def parse_first_post_from_text(body_text: str) -> str | None:
    t = (body_text or "").replace("\r\n", "\n").replace("\r", "\n")
    m = RE_FIRSTPOST_ANY.search(t)
    if not m:
        return None
    y, mo, d, hh, mm, ss = m.groups()
    return f"{y}-{mo}-{d} {hh}:{mm}:{ss}"


def should_out_auto(title: str) -> int:
    t = title or ""
    if "PART" in t.upper():
        return 1
    for w in OUT_AUTO_WORDS:
        if w in ("Part", "PART"):
            continue
        if w and (w in t):
            return 1
    return 0


def build_page_url(cfg: CategoryConfig, page_no: int) -> str:
    return f"{cfg.base_url}/{page_no}/" + (cfg.params or "")


def resolve_enabled_categories() -> List[CategoryConfig]:
    enabled: List[CategoryConfig] = []
    for i in ENABLED_CATEGORY_INDEXES:
        if 0 <= i < len(CATEGORIES):
            enabled.append(CATEGORIES[i])
        else:
            print(f"[WARN] ENABLED_CATEGORY_INDEXES に範囲外 index={i}（無視）")
    return enabled


def short(s: str, n: int = 70) -> str:
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def fetch_first_post_via_comment1(detail_page, thread_code: str) -> str | None:
    url = f"https://girlschannel.net/comment/{thread_code}/1/"
    try:
        resp = detail_page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS_FIRSTPOST_GOTO)
        status = resp.status if resp else None
        if not resp or (status and status >= 400):
            return None
    except PWTimeoutError:
        return None

    try:
        body_txt = detail_page.locator("body").inner_text(timeout=TIMEOUT_MS_FIRSTPOST_TEXT)
    except PWTimeoutError:
        return None
    except Exception:
        return None

    return parse_first_post_from_text(body_txt)


def install_request_blocking(page) -> None:
    if not REQUEST_BLOCK:
        return

    def _route_handler(route, request):
        rtype = request.resource_type
        if rtype in ("image", "media", "font", "stylesheet"):
            return route.abort()
        return route.continue_()

    try:
        page.route("**/*", _route_handler)
    except Exception:
        pass


# =========================================================
# メイン
# =========================================================
def main() -> int:
    if TARGET_NEW_COUNT <= 0:
        print("TARGET_NEW_COUNT は 1以上にしてください")
        return 2
    if PAGE_FROM <= 0 or PAGE_TO <= 0 or PAGE_TO < PAGE_FROM:
        print("PAGE_FROM/PAGE_TO の指定が不正です")
        return 2
    if MIN_COMMENTS < 0:
        print("MIN_COMMENTS は 0以上にしてください")
        return 2

    enabled_categories = resolve_enabled_categories()
    if not enabled_categories:
        print("[ERROR] 取得対象カテゴリが0件です。ENABLED_CATEGORY_INDEXES を設定してください。")
        return 2

    con = connect(DB_PATH)
    run_dt = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")

    saved = 0
    seen = 0
    under_min = 0
    failed_item = 0
    failed_page = 0
    new_inserts = 0
    updated_ct = 0
    out_auto_ones = 0

    newly_inserted_codes: List[str] = []
    first_post_filled = 0
    first_post_failed = 0
    first_post_skipped = 0

    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] tables: {TABLE_ALL}, {TABLE_DO}, {TABLE_DONE}")
    print(f"[INFO] run_dt: {run_dt}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(locale="ja-JP")
        page = context.new_page()
        detail_page = context.new_page()

        install_request_blocking(page)
        install_request_blocking(detail_page)

        try:
            pbar = tqdm(total=TARGET_NEW_COUNT, desc="saved")

            # STEP 1: list -> items_all
            for cfg in enabled_categories:
                if saved >= TARGET_NEW_COUNT:
                    break

                print(f"\n[CATEGORY] {cfg.name}")
                consecutive_no_save_pages = 0

                for page_no in range(PAGE_FROM, PAGE_TO + 1):
                    if saved >= TARGET_NEW_COUNT:
                        break

                    url = build_page_url(cfg, page_no)

                    try:
                        resp = page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS_LIST)
                        status = resp.status if resp else None
                        if (not resp) or (status and status >= 400):
                            failed_page += 1
                            print(f"[PAGE_FAIL] cat={cfg.name} page={page_no} status={status} url={url}")
                            time.sleep(SLEEP_SEC)
                            continue
                    except PWTimeoutError:
                        failed_page += 1
                        print(f"[PAGE_TIMEOUT] cat={cfg.name} page={page_no} url={url}")
                        time.sleep(SLEEP_SEC)
                        continue

                    li = page.locator("xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li")
                    li_count = li.count()
                    if li_count == 0:
                        break

                    page_rows: List[Tuple[int, Dict[str, Any], bool]] = []

                    for idx in range(1, li_count + 1):
                        seen += 1

                        a = page.locator(f"xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li[{idx}]/a").first
                        href = a.get_attribute("href") or ""
                        m = RE_TOPIC_HREF.search(href)
                        if not m:
                            failed_item += 1
                            continue
                        code = m.group(1)

                        try:
                            comments_raw = page.locator(
                                f"xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li[{idx}]/a/div/p/span[2]"
                            ).first.inner_text(timeout=5000).strip()
                            last_raw = page.locator(
                                f"xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li[{idx}]/a/div/p/span[3]"
                            ).first.inner_text(timeout=5000).strip()
                            title = page.locator(
                                f"xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li[{idx}]/a/p"
                            ).first.inner_text(timeout=5000).strip()
                        except PWTimeoutError:
                            failed_item += 1
                            continue

                        comments_count = digits_only_int(comments_raw)
                        if comments_count < MIN_COMMENTS:
                            under_min += 1
                            continue

                        already = exists_code(con, code)
                        if (not UPDATE_EXISTING) and already:
                            continue

                        last_post = normalize_list_datetime(last_raw)
                        out_auto = should_out_auto(title)

                        row: Dict[str, Any] = {
                            "code": code,
                            "check_date": run_dt,
                            "first_seen_at": run_dt,
                            "first_post": None,
                            "last_post": last_post,
                            "comments_count": comments_count,
                            "category": cfg.name,
                            "title": title,
                            "out_auto": out_auto,
                        }
                        page_rows.append((idx, row, already))

                    # そのページ内の並びを安定化（コメント数降順→last_post昇順）
                    page_rows.sort(key=lambda t: (-int(t[1]["comments_count"]), str(t[1]["last_post"])))

                    page_saved = 0
                    for orig_idx, row, already in page_rows:
                        if saved >= TARGET_NEW_COUNT:
                            break
                        upsert_items_all(con, row)
                        con.commit()

                        saved += 1
                        page_saved += 1
                        pbar.update(1)

                        if row["out_auto"] == 1:
                            out_auto_ones += 1

                        if already:
                            updated_ct += 1
                        else:
                            new_inserts += 1
                            newly_inserted_codes.append(row["code"])

                        if ECHO_EACH_SAVE:
                            print(
                                f"[OK] {cfg.name} p{page_no} li{orig_idx} code={row['code']} "
                                f"c={row['comments_count']} out_auto={row['out_auto']} "
                                f"title={short(row['title'], 60)}"
                            )

                    if page_saved == 0:
                        consecutive_no_save_pages += 1
                        if EARLY_STOP_PAGES > 0 and consecutive_no_save_pages >= EARLY_STOP_PAGES:
                            break
                    else:
                        consecutive_no_save_pages = 0

                    time.sleep(SLEEP_SEC)

            pbar.close()

            # STEP 2: first_post（新規＆excluded=0のみ）
            print("\n[STEP2] fetch first_post (newly inserted & excluded=0)")
            for code in newly_inserted_codes:
                if get_excluded(con, code) == 1:
                    first_post_skipped += 1
                    continue
                fp = fetch_first_post_via_comment1(detail_page, code)
                if fp:
                    set_first_post_if_empty(con, code, fp)
                    con.commit()
                    first_post_filled += 1
                else:
                    first_post_failed += 1
                if DETAIL_SLEEP_SEC > 0:
                    time.sleep(DETAIL_SLEEP_SEC)

            # STEP 2.5: comment_average
            print("\n[STEP2.5] recompute comment_average")
            recompute_comment_average(con)

            # STEP 3: items_do sync + priority
            print("\n[STEP3] sync items_do + priority")
            sync_items_do_from_all(con)

        finally:
            con.close()
            context.close()
            browser.close()

    print("\n[SUMMARY]")
    print(f"  saved={saved}/{TARGET_NEW_COUNT} new={new_inserts} updated={updated_ct}")
    print(f"  seen={seen} under_min={under_min} failed_item={failed_item} failed_page={failed_page}")
    print(f"  out_auto=1 count={out_auto_ones}")
    print(f"  first_post: filled={first_post_filled} failed={first_post_failed} skipped_excluded={first_post_skipped}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
