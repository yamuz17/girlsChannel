#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

# テーブル名（★指定どおり）
TABLE_ALL  = "items_all"
TABLE_DO   = "items_do"
TABLE_DONE = "items_done"

# 取得ページ範囲（カテゴリ一覧）
PAGE_FROM = 1
PAGE_TO   = 15

# 保存条件
TARGET_NEW_COUNT = 1000          # 全カテゴリ合算で、この件数保存したら終了
MIN_COMMENTS = 400               # このコメント数以上だけ保存
UPDATE_EXISTING = True           # 既存Codeも更新する（最新状態保持なのでTrue推奨）

# Playwright
HEADLESS = True
TIMEOUT_MS_LIST = 30000          # 一覧ページはそこそこ長め
TIMEOUT_MS_FIRSTPOST_GOTO = 12000
TIMEOUT_MS_FIRSTPOST_TEXT = 3000
SLEEP_SEC = 0.6                  # 一覧ページ間隔
REQUEST_BLOCK = True             # 画像/CSS/フォント等をブロックして高速化
DETAIL_SLEEP_SEC = 0.05          # first_post 取得間隔（短め）

# ログ/早期終了
ECHO_EACH_SAVE = True
EARLY_STOP_PAGES = 2             # “保存0件ページ” が連続したらカテゴリを打ち切り（0で無効）

# -----------------------------
# out_auto 判定用語（= out_auto=1 → excluded=1）
# -----------------------------
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

# -----------------------------
# 取得するURLを個別に切り替える（0-based）
# 例）2つ目だけ取得 → ENABLED_CATEGORY_INDEXES = [1]
# -----------------------------
#ENABLED_CATEGORY_INDEXES: List[int] = [0, 1, 2]
ENABLED_CATEGORY_INDEXES: List[int] = [0, 1]

# =========================================================
# 正規表現
# =========================================================
RE_TOPIC_HREF = re.compile(r"/topics/(\d+)/")

# commentページの本文から、最初に見つかった日時っぽい文字列を拾う（例: 2026/01/03(土) 09:26:43）
RE_FIRSTPOST_ANY = re.compile(
    r"(\d{4})/(\d{2})/(\d{2}).*?(\d{2}):(\d{2}):(\d{2})"
)

# =========================================================
# 取得対象カテゴリ
# =========================================================
@dataclass(frozen=True)
class CategoryConfig:
    name: str
    base_url: str
    params: str

CATEGORIES: List[CategoryConfig] = [
    CategoryConfig(
        name="ゴシップ",
        base_url="https://girlschannel.net/topics/category/gossip",
        params="?sort=comment&date=m",
    ),
    CategoryConfig(
        name="ニュース",
        base_url="https://girlschannel.net/topics/category/news",
        params="?sort=comment&date=m",
    ),
    CategoryConfig(
        name="政治経済",
        base_url="https://girlschannel.net/topics/category/politics",
        params="?sort=comment&date=m",
    ),
]

# =========================================================
# DDL（items_all）
#  - スレッドIDは code 列に格納
#  - id は INTEGER PRIMARY KEY（自動採番）
#  - code は UNIQUE（items_all だけ code の一意性を担保）
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
  excluded INTEGER NOT NULL DEFAULT 0
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
CREATE INDEX IF NOT EXISTS idx_{TABLE_ALL}_sort_cc_desc_lp_asc
  ON {TABLE_ALL}(comments_count DESC, last_post ASC);
"""

# =========================================================
# DDL（items_do / items_done）
#  ★ユーザー指定:
#   - テーブル名：items_do / items_done
#   - do: id カラムを PRIMARY KEY、created_at/update_at は不要
#   - done: id カラムを PRIMARY KEY
#
#  実運用上、同期(upsert)のため code を UNIQUE にする（禁止されていないので採用）
# =========================================================
DDL_DO_DONE = f"""
CREATE TABLE IF NOT EXISTS {TABLE_DO} (
  id INTEGER PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,              -- スレッドID（items_all.code）: 同期のため一意
  title TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',  -- queued / working / done 等（運用で拡張OK）
  step INTEGER NOT NULL DEFAULT 0,        -- 工程番号（運用で定義）
  last_error TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_{TABLE_DO}_status ON {TABLE_DO}(status);

CREATE TABLE IF NOT EXISTS {TABLE_DONE} (
  id INTEGER PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,              -- スレッドID（items_all.code）: 1動画=1投稿管理を想定
  title TEXT NOT NULL,

  done_at TEXT NOT NULL DEFAULT (datetime('now')),   -- 完了時刻（不要なら後で削除OK）
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),-- 更新時刻（投稿ステータスで更新）

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
# DBユーティリティ
# =========================================================
def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path), timeout=30)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    ensure_schema(con)
    return con

def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None

def _colnames(con: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table});").fetchall()]

def ensure_schema(con: sqlite3.Connection) -> None:
    """
    1) items_all を最新スキーマで用意（必要なら旧スキーマから移行）
    2) items_do / items_done を作成
    3) excluded 同期トリガーを再作成
    """
    # --- 旧スキーマ判定（items_all） ---
    if _table_exists(con, TABLE_ALL):
        cols = _colnames(con, TABLE_ALL)
        colset = set(cols)

        # 旧: id(TEXT PK) で code が無い → 新スキーマへ移行
        if ("code" not in colset) and ("id" in colset):
            # point_out -> out_auto
            if "point_out" in colset and "out_auto" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} RENAME COLUMN point_out TO out_auto;")
                con.commit()
                cols = _colnames(con, TABLE_ALL)
                colset = set(cols)

            # post_date -> last_post
            if "post_date" in colset and "last_post" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} RENAME COLUMN post_date TO last_post;")
                con.commit()
                cols = _colnames(con, TABLE_ALL)
                colset = set(cols)

            if "first_seen_at" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN first_seen_at TEXT;")
                con.commit()
                colset.add("first_seen_at")
            if "first_post" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN first_post TEXT;")
                con.commit()
                colset.add("first_post")
            if "last_post" not in colset:
                con.execute(
                    f"ALTER TABLE {TABLE_ALL} ADD COLUMN last_post TEXT NOT NULL DEFAULT '1970-01-01 00:00:00';"
                )
                con.commit()
                colset.add("last_post")
            if "out_auto" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN out_auto INTEGER NOT NULL DEFAULT 0;")
                con.commit()
                colset.add("out_auto")
            if "out_manual" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN out_manual INTEGER NOT NULL DEFAULT 0;")
                con.commit()
                colset.add("out_manual")
            if "excluded" not in colset:
                con.execute(f"ALTER TABLE {TABLE_ALL} ADD COLUMN excluded INTEGER NOT NULL DEFAULT 0;")
                con.commit()
                colset.add("excluded")

            # first_seen_at が空なら check_date を入れておく
            con.execute(
                f"""
                UPDATE {TABLE_ALL}
                   SET first_seen_at = check_date
                 WHERE (first_seen_at IS NULL OR first_seen_at = '')
                """
            )
            con.commit()

            # excluded を out_auto/out_manual から再計算
            con.execute(
                f"""
                UPDATE {TABLE_ALL}
                   SET excluded = CASE WHEN COALESCE(out_auto,0)=1 OR COALESCE(out_manual,0)=1 THEN 1 ELSE 0 END
                """
            )
            con.commit()

            # --- 新テーブルにコピー ---
            tmp = f"{TABLE_ALL}__new"
            con.execute(f"DROP TABLE IF EXISTS {tmp};")
            con.commit()

            # 新スキーマ作成（tmp名）
            con.executescript(
                DDL_ITEMS_ALL.replace(
                    f"CREATE TABLE IF NOT EXISTS {TABLE_ALL}",
                    f"CREATE TABLE IF NOT EXISTS {tmp}"
                )
            )
            con.commit()

            # 旧 id(TEXT) → 新 code(TEXT UNIQUE)
            con.execute(
                f"""
                INSERT INTO {tmp} (
                  code, check_date, first_seen_at, first_post, last_post,
                  comments_count, category, title, out_auto, out_manual, excluded
                )
                SELECT
                  id AS code,
                  check_date, first_seen_at, first_post, last_post,
                  comments_count, category, title,
                  COALESCE(out_auto,0), COALESCE(out_manual,0), COALESCE(excluded,0)
                FROM {TABLE_ALL}
                """
            )
            con.commit()

            # 旧テーブルを入れ替え
            con.execute(f"DROP TABLE {TABLE_ALL};")
            con.execute(f"ALTER TABLE {tmp} RENAME TO {TABLE_ALL};")
            con.commit()

    # --- 最新DDLを適用（無ければ作る / index補完） ---
    con.executescript(DDL_ITEMS_ALL)
    con.executescript(DDL_DO_DONE)
    con.commit()

    # --- excluded 同期トリガー（items_all: code 版） ---
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
    if not row:
        return 0
    return int(row[0] or 0)

def set_first_post(con: sqlite3.Connection, code: str, first_post: str) -> None:
    con.execute(
        f"""
        UPDATE {TABLE_ALL}
           SET first_post = COALESCE(first_post, ?)
         WHERE code=?;
        """,
        (first_post, code),
    )

def upsert(con: sqlite3.Connection, row: Dict[str, Any]) -> None:
    """
    - first_seen_at は初回INSERTで入れる / 以後保持（NULLのときだけ埋める）
    - first_post も同様に “NULLのときだけ埋める”（後段で入れる）
    - last_post は毎回更新
    - out_manual は手動なのでUPSERT更新で触らない（=消さない）
    - excluded はトリガーで out_auto/out_manual から自動同期
    """
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
    con.execute(sql, (
        row["code"],
        row["check_date"],
        row["first_seen_at"],
        row.get("first_post"),
        row["last_post"],
        int(row["comments_count"]),
        row["category"],
        row["title"],
        int(row["out_auto"]),
    ))

# =========================================================
# items_do 同期（items_all -> items_do）
#   - excluded=1 は items_do から削除（履歴残さない）
#   - created_at/updated_at は無いので title だけ追随させる
# =========================================================
def sync_items_do_from_all(con: sqlite3.Connection) -> None:
    """
    items_all が更新されたら items_do も同期する。

    仕様:
    - items_all.excluded=0 の code/title を items_do へ投入
    - 既に items_do にある code は title を反映（status/step/エラーは維持）
    - items_all.excluded=1 は items_do から削除
    - items_all から消えた code も items_do から削除
    """
    # 1) excluded=0 を追加/同期（status/stepは上書きしない）
    con.execute(
        f"""
        INSERT INTO {TABLE_DO} (code, title, status, step)
        SELECT a.code, a.title, 'queued', 0
          FROM {TABLE_ALL} a
         WHERE a.excluded = 0
        ON CONFLICT(code) DO UPDATE SET
          title=excluded.title
        """
    )
    upsert_changes = con.execute("SELECT changes();").fetchone()[0]

    # 2) excluded=1 は削除
    con.execute(
        f"""
        DELETE FROM {TABLE_DO}
         WHERE code IN (SELECT code FROM {TABLE_ALL} WHERE excluded=1)
        """
    )
    deleted_excluded = con.execute("SELECT changes();").fetchone()[0]

    # 3) items_all から消えた code も削除
    con.execute(
        f"""
        DELETE FROM {TABLE_DO}
         WHERE code NOT IN (SELECT code FROM {TABLE_ALL})
        """
    )
    deleted_missing = con.execute("SELECT changes();").fetchone()[0]

    con.commit()

    total_do = con.execute(f"SELECT COUNT(*) FROM {TABLE_DO};").fetchone()[0]
    print(f"[SYNC] {TABLE_DO}: upsert_changes={upsert_changes} deleted_excluded={deleted_excluded} deleted_missing={deleted_missing} total={total_do}")

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
    t = (title or "")
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

# =========================================================
# first_post 取得（高速版：comment/1 を参照）
# =========================================================
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

# =========================================================
# Playwright高速化：リクエストブロック
# =========================================================
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
        print("  例）2つ目だけ → ENABLED_CATEGORY_INDEXES = [1]")
        return 2

    con = connect(DB_PATH)

    run_dt = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")

    saved = 0
    seen = 0
    skipped_under_min = 0
    failed_item = 0
    failed_page = 0

    new_inserts = 0
    updated = 0
    out_auto_ones = 0

    newly_inserted_codes: List[str] = []

    first_post_filled = 0
    first_post_skipped_excluded = 0
    first_post_failed = 0

    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] table_all: {TABLE_ALL}")
    print(f"[INFO] table_do: {TABLE_DO}")
    print(f"[INFO] table_done: {TABLE_DONE}")
    print(f"[INFO] run_dt: {run_dt}")
    print(f"[INFO] page_from..to: {PAGE_FROM}..{PAGE_TO}")
    print(f"[INFO] target_save(total): {TARGET_NEW_COUNT}")
    print(f"[INFO] min_comments: {MIN_COMMENTS}")
    print(f"[INFO] update_existing: {UPDATE_EXISTING}")
    print(f"[INFO] enabled_categories: {', '.join([c.name for c in enabled_categories])}")
    print(f"[INFO] first_post: comment/1 から取得（excluded=1はスキップ）")
    print(f"[INFO] request_block: {REQUEST_BLOCK}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(locale="ja-JP")

        page = context.new_page()
        detail_page = context.new_page()

        install_request_blocking(page)
        install_request_blocking(detail_page)

        try:
            pbar = tqdm(total=TARGET_NEW_COUNT, desc="saved")

            # STEP 1) 一覧取得 → items_all へ保存
            for cfg in enabled_categories:
                if saved >= TARGET_NEW_COUNT:
                    break

                print(f"\n[CATEGORY] {cfg.name}  base={cfg.base_url}  params={cfg.params}")
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

                    li_locator = page.locator("xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li")
                    li_count = li_locator.count()
                    if li_count == 0:
                        print(f"[NO_ITEMS] cat={cfg.name} page={page_no} url={url}")
                        break

                    page_rows: List[Tuple[int, Dict[str, Any], bool]] = []

                    for idx in range(1, li_count + 1):
                        seen += 1

                        a = page.locator(
                            f"xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li[{idx}]/a"
                        ).first
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
                            skipped_under_min += 1
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

                    page_rows.sort(key=lambda t: (-int(t[1]["comments_count"]), str(t[1]["last_post"])))

                    page_saved = 0
                    for orig_idx, row, already in page_rows:
                        if saved >= TARGET_NEW_COUNT:
                            break

                        upsert(con, row)
                        con.commit()

                        saved += 1
                        page_saved += 1
                        pbar.update(1)

                        if row["out_auto"] == 1:
                            out_auto_ones += 1

                        if already:
                            updated += 1
                        else:
                            new_inserts += 1
                            newly_inserted_codes.append(row["code"])

                        if ECHO_EACH_SAVE:
                            print(
                                f"[OK] cat={cfg.name} page={page_no} li={orig_idx} saved={saved} code={row['code']} "
                                f"last={row['last_post']} c={row['comments_count']} out_auto={row['out_auto']} "
                                f"new={'1' if not already else '0'} "
                                f"title={short(row['title'],60)}"
                            )

                    if page_saved == 0:
                        consecutive_no_save_pages += 1
                        print(
                            f"[NO_SAVE] cat={cfg.name} page={page_no} consecutive={consecutive_no_save_pages} "
                            f"(under_min_total={skipped_under_min}, failed_total={failed_item})"
                        )
                        if EARLY_STOP_PAGES > 0 and consecutive_no_save_pages >= EARLY_STOP_PAGES:
                            print("[EARLY_STOP] no saved items for consecutive pages (this category) -> stop this category")
                            break
                    else:
                        consecutive_no_save_pages = 0

                    time.sleep(SLEEP_SEC)

            pbar.close()

            # STEP 2) 新規 & excluded=0 のみ first_post を埋める
            if newly_inserted_codes:
                print("\n[STEP2] fetch first_post for newly inserted & excluded=0 (via comment/1)")
                print(f"[STEP2] newly_inserted_codes={len(newly_inserted_codes)}")
            else:
                print("\n[STEP2] no newly inserted codes -> skip first_post fetching")

            for i, code in enumerate(newly_inserted_codes, start=1):
                if get_excluded(con, code) == 1:
                    first_post_skipped_excluded += 1
                    continue

                fp = fetch_first_post_via_comment1(detail_page, code)
                if fp:
                    set_first_post(con, code, fp)
                    con.commit()
                    first_post_filled += 1
                else:
                    first_post_failed += 1

                if (i % 50) == 0:
                    print(f"[STEP2] progress {i}/{len(newly_inserted_codes)} filled={first_post_filled} failed={first_post_failed} skipped_excl={first_post_skipped_excluded}")

                if DETAIL_SLEEP_SEC > 0:
                    time.sleep(DETAIL_SLEEP_SEC)

            # STEP 3) items_all -> items_do 同期（excluded=0 の code/title 抽出）
            print("\n[STEP3] sync items_do from items_all (excluded=0)")
            sync_items_do_from_all(con)

        finally:
            con.close()
            context.close()
            browser.close()

    print("\n[SUMMARY]")
    print(f"  saved={saved} target={TARGET_NEW_COUNT}")
    print(f"  new_inserts={new_inserts} updated={updated}")
    print(f"  seen={seen} under_min={skipped_under_min} failed_item={failed_item} failed_page={failed_page}")
    print(f"  out_auto=1 count={out_auto_ones}")
    print(f"  first_post_filled={first_post_filled}")
    print(f"  first_post_failed={first_post_failed}")
    print(f"  first_post_skipped_excluded={first_post_skipped_excluded}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
