#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import re
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
from dataclasses import dataclass

from tqdm import tqdm
from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# =========================================================
# 設定（ここだけ変えればOK）
# =========================================================
BASE_DIR = Path("/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001")

# ★DBファイル
DB_PATH = BASE_DIR / "list_category_gossip.db"

PAGE_FROM = 1
PAGE_TO   = 15

TARGET_NEW_COUNT = 1000          # 追加保存（新規 or 更新）した件数がこれに達したら終了（全カテゴリ合算）
MIN_COMMENTS = 500              # ★このコメント数以上だけ保存
UPDATE_EXISTING = False          # ★既存IDも更新するならTrue（基本False推奨）

HEADLESS = True
SLEEP_SEC = 0.6
TIMEOUT_MS = 30000

ECHO_EACH_SAVE = True            # 保存ごとにターミナル表示
EARLY_STOP_PAGES = 2             # 保存0件ページが連続したら終了（0で無効）※カテゴリごとに判定

# 投稿用タイトルだけ作る（post_tags / post_desc は廃止）
ENABLE_POST_TITLE = True

# ★タグ学習用カラム（このスクリプトは作成だけ。値更新は別スクリプト担当）
ENABLE_TAG_LEARNING_COLUMNS = True

# ★取得対象カテゴリ（追加：ニュース/政治経済）
# - gossip は現状維持（sort=comment, date=y）
# - news/politics は要件通り（sort=comment, date=w）
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

DDL = """
CREATE TABLE IF NOT EXISTS items (
  id TEXT PRIMARY KEY,
  check_create INTEGER NOT NULL DEFAULT 0,   -- 0:未処理 / 1:処理対象 / 2:完了
  check_date TEXT NOT NULL,
  post_date TEXT NOT NULL,
  comments_count INTEGER NOT NULL,
  category TEXT NOT NULL,
  title TEXT NOT NULL,
  post_title TEXT,
  keywords_raw TEXT,
  keywords_keep TEXT,
  keywords_drop TEXT
);

CREATE INDEX IF NOT EXISTS idx_items_post_date ON items(post_date);
CREATE INDEX IF NOT EXISTS idx_items_check_date ON items(check_date);
CREATE INDEX IF NOT EXISTS idx_items_comments ON items(comments_count);
CREATE INDEX IF NOT EXISTS idx_items_check_create ON items(check_create);

-- ★ソート用（コメント数: 多い順 / post_date: 古い順）
-- SQLiteは ASC/DESC 付きインデックスをサポートします。
CREATE INDEX IF NOT EXISTS idx_items_sort_cc_desc_pd_asc
  ON items(comments_count DESC, post_date ASC);
"""

RE_TOPIC_HREF = re.compile(r"/topics/(\d+)/")

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path), timeout=30)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.executescript(DDL)
    con.commit()
    return con

def ensure_columns(con: sqlite3.Connection) -> None:
    """
    既存DBにも安全に追記できるように、カラムが無ければALTERで追加。
    ※SQLiteは列の物理順を入れ替えられないので「無ければ足す」でOK。
    """
    cols = {r[1] for r in con.execute("PRAGMA table_info(items)").fetchall()}

    # post_title（残す）
    if "post_title" not in cols:
        con.execute("ALTER TABLE items ADD COLUMN post_title TEXT;")

    # ★check_create（デフォルト0）
    if "check_create" not in cols:
        con.execute("ALTER TABLE items ADD COLUMN check_create INTEGER NOT NULL DEFAULT 0;")

    # NULLの可能性があるので0埋め
    con.execute("UPDATE items SET check_create=0 WHERE check_create IS NULL;")

    # ★タグ学習用3カラム
    if ENABLE_TAG_LEARNING_COLUMNS:
        for name in ("keywords_raw", "keywords_keep", "keywords_drop"):
            if name not in cols:
                con.execute(f"ALTER TABLE items ADD COLUMN {name} TEXT;")

    con.commit()

def exists_id(con: sqlite3.Connection, tid: str) -> bool:
    return con.execute("SELECT 1 FROM items WHERE id=? LIMIT 1", (tid,)).fetchone() is not None

def upsert(con: sqlite3.Connection, row: Dict[str, Any]) -> None:
    """
    注意:
    - check_create は “新規INSERT時のみ” row側（基本0）を入れる
    - 既存UPDATE時は check_create を上書きしない（別スクリプトの判定に使うため）
    - keywords_* もこのスクリプトでは触らない（別スクリプト担当）
    """
    sql = """
    INSERT INTO items (id, check_create, check_date, post_date, comments_count, category, title, post_title)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      check_date=excluded.check_date,
      post_date=excluded.post_date,
      comments_count=excluded.comments_count,
      category=excluded.category,
      title=excluded.title,
      post_title=excluded.post_title
    """
    con.execute(sql, (
        row["id"],
        int(row.get("check_create", 0)),
        row["check_date"],
        row["post_date"],
        int(row["comments_count"]),
        row["category"],
        row["title"],
        row.get("post_title"),
    ))

def digits_only_int(s: str) -> int:
    nums = re.findall(r"\d+", (s or "").replace(",", ""))
    return int("".join(nums)) if nums else 0

def normalize_post_date(raw: str) -> str:
    txt = (raw or "").strip()
    if not txt:
        return "1970-01-01 00:00:00"
    try:
        dt = dtparser.parse(txt, fuzzy=True)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return txt

def short(s: str, n: int = 70) -> str:
    s = (s or "").replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "…"

def build_page_url(cfg: CategoryConfig, page_no: int) -> str:
    return f"{cfg.base_url}/{page_no}/" + (cfg.params or "")

def build_post_title(title: str) -> str:
    raw = (title or "").strip()
    core = re.sub(r"【[^】]{1,30}】", "", raw).strip()
    core = re.sub(r"\b(PART|Part|part)\s*\d+\b", "", core).strip()
    core = re.sub(r"[#＃]\s*\d+\b", "", core).strip()
    core = re.sub(r"(第\s*\d+\s*(回|弾))", "", core).strip()
    core = re.sub(r"(パート|Part|PART)\s*\d+\s*$", "", core).strip()
    core = re.sub(r"\s{2,}", " ", core).strip(" 　-–—_:：")
    return core if core else raw

def main():
    if TARGET_NEW_COUNT <= 0:
        raise SystemExit("TARGET_NEW_COUNT は 1以上にしてください")
    if PAGE_FROM <= 0 or PAGE_TO <= 0 or PAGE_TO < PAGE_FROM:
        raise SystemExit("PAGE_FROM/PAGE_TO の指定が不正です")
    if MIN_COMMENTS < 0:
        raise SystemExit("MIN_COMMENTS は 0以上にしてください")
    if not CATEGORIES:
        raise SystemExit("CATEGORIES が空です")

    con = connect(DB_PATH)
    ensure_columns(con)

    check_date = datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d")

    saved = 0
    pages_done = 0
    seen = 0
    skipped_exists = 0
    skipped_under_min = 0
    failed_item = 0
    failed_page = 0

    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] check_date: {check_date}")
    print(f"[INFO] page_from..to: {PAGE_FROM}..{PAGE_TO}")
    print(f"[INFO] target_save(total): {TARGET_NEW_COUNT}")
    print(f"[INFO] min_comments: {MIN_COMMENTS}")
    print(f"[INFO] update_existing: {UPDATE_EXISTING}")
    print(f"[INFO] categories: {', '.join([c.name for c in CATEGORIES])}")
    for c in CATEGORIES:
        print(f"  - {c.name}: {c.base_url}/?{(c.params or '').lstrip('?')}")
    print(f"[INFO] early_stop_pages(per_category): {EARLY_STOP_PAGES}")
    print(f"[INFO] post_title: {ENABLE_POST_TITLE}")
    print("[INFO] check_create: 0=未処理 / 1=処理対象 / 2=完了（※このスクリプトは新規0、既存は上書きしない）")
    print("[INFO] sort_rule: comments_count DESC, post_date ASC (ページ内で整列してUPSERT)")
    if ENABLE_TAG_LEARNING_COLUMNS:
        print("[INFO] tag_learning_columns: keywords_raw / keywords_keep / keywords_drop (added if missing)")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(locale="ja-JP")
        page = context.new_page()

        try:
            pbar = tqdm(total=TARGET_NEW_COUNT, desc="saved")

            # ★カテゴリごとに巡回
            for cfg in CATEGORIES:
                if saved >= TARGET_NEW_COUNT:
                    break

                print(f"\n[CATEGORY] {cfg.name}  base={cfg.base_url}  params={cfg.params}")
                consecutive_no_save_pages = 0  # カテゴリごとにリセット

                for page_no in range(PAGE_FROM, PAGE_TO + 1):
                    if saved >= TARGET_NEW_COUNT:
                        break

                    pages_done += 1
                    url = build_page_url(cfg, page_no)

                    try:
                        resp = page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
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

                    # ★ページ内を「コメント多い順 → post_date古い順」で整列してから保存
                    page_rows: List[Tuple[int, Dict[str, Any]]] = []

                    for idx in range(1, li_count + 1):
                        seen += 1

                        a = page.locator(f"xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li[{idx}]/a").first
                        href = a.get_attribute("href") or ""
                        m = RE_TOPIC_HREF.search(href)
                        if not m:
                            failed_item += 1
                            continue
                        tid = m.group(1)

                        try:
                            comments_raw = page.locator(
                                f"xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li[{idx}]/a/div/p/span[2]"
                            ).first.inner_text(timeout=5000).strip()
                            post_raw = page.locator(
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

                        if (not UPDATE_EXISTING) and exists_id(con, tid):
                            skipped_exists += 1
                            continue

                        post_date = normalize_post_date(post_raw)

                        row: Dict[str, Any] = {
                            "id": tid,
                            "check_create": 0,     # 新規は必ず0
                            "check_date": check_date,
                            "post_date": post_date,
                            "comments_count": comments_count,
                            "category": cfg.name,  # ★カテゴリ名をここで付与
                            "title": title,
                            "post_title": None,
                        }
                        if ENABLE_POST_TITLE:
                            row["post_title"] = build_post_title(title)

                        page_rows.append((idx, row))

                    # ソート: comments_count DESC, post_date ASC
                    page_rows.sort(key=lambda t: (-int(t[1]["comments_count"]), str(t[1]["post_date"])))

                    page_saved = 0
                    for orig_idx, row in page_rows:
                        if saved >= TARGET_NEW_COUNT:
                            break

                        upsert(con, row)
                        con.commit()

                        saved += 1
                        page_saved += 1
                        pbar.update(1)

                        if ECHO_EACH_SAVE:
                            print(
                                f"[OK] cat={cfg.name} page={page_no} li={orig_idx} saved={saved} id={row['id']} "
                                f"post={row['post_date']} c={row['comments_count']} "
                                f"title={short(row['title'],60)} "
                                f"post_title={short(row.get('post_title') or '',40)} "
                                f"check_create=0"
                            )

                    if page_saved == 0:
                        consecutive_no_save_pages += 1
                        print(
                            f"[NO_SAVE] cat={cfg.name} page={page_no} consecutive={consecutive_no_save_pages} "
                            f"(under_min_total={skipped_under_min}, exists_total={skipped_exists}, failed_total={failed_item})"
                        )
                        if EARLY_STOP_PAGES > 0 and consecutive_no_save_pages >= EARLY_STOP_PAGES:
                            print("[EARLY_STOP] no saved items for consecutive pages (this category) -> stop this category")
                            break
                    else:
                        consecutive_no_save_pages = 0

                    time.sleep(SLEEP_SEC)

            pbar.close()

        finally:
            con.close()
            context.close()
            browser.close()

    print("\n[SUMMARY]")
    print(f"  saved={saved} target={TARGET_NEW_COUNT}")
    print(f"  pages_done={pages_done} range={PAGE_FROM}..{PAGE_TO}  categories={len(CATEGORIES)}")
    print(f"  seen={seen} under_min={skipped_under_min} skipped_exists={skipped_exists} failed_item={failed_item} failed_page={failed_page}")
    if saved == 0:
        print("  [WARN] 保存が0件です。MIN_COMMENTSが高すぎる/ページ範囲が新しすぎる可能性があります。")

if __name__ == "__main__":
    main()
