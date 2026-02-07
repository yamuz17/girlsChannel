#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import re
import time
from pathlib import Path
from typing import Dict, Any, Optional
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

from tqdm import tqdm
from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# =========================================================
# 設定（ここだけ変えればOK）
# =========================================================
BASE_DIR = Path("/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001")
DB_PATH = BASE_DIR / "list_category_tv.db"

BASE_URL = "https://girlschannel.net/topics/category/tv"
PARAMS = "?sort=&date=y"

PAGE_FROM = 1
PAGE_TO   = 30

TARGET_NEW_COUNT = 1000          # 追加保存（新規 or 更新）した件数がこれに達したら終了

CATEGORY_NAME = "テレビ・CM"

MIN_COMMENTS = 1000              # ★このコメント数以上だけ保存
UPDATE_EXISTING = False          # ★既存IDも更新するならTrue（基本False推奨）

HEADLESS = True
SLEEP_SEC = 0.6
TIMEOUT_MS = 30000

ECHO_EACH_SAVE = True            # 保存ごとにターミナル表示
EARLY_STOP_PAGES = 2             # 保存0件ページが連続したら終了（0で無効）

# 投稿用フィールド
ENABLE_POST_FIELDS = True        # 投稿用タイトル/タグ/概要欄を作る
# =========================================================

DDL = """
CREATE TABLE IF NOT EXISTS items (
  id TEXT PRIMARY KEY,
  check_date TEXT NOT NULL,
  post_date TEXT NOT NULL,
  comments_count INTEGER NOT NULL,
  category TEXT NOT NULL,
  title TEXT NOT NULL,
  post_title TEXT,
  post_tags TEXT,
  post_desc TEXT
);
CREATE INDEX IF NOT EXISTS idx_items_post_date ON items(post_date);
CREATE INDEX IF NOT EXISTS idx_items_check_date ON items(check_date);
CREATE INDEX IF NOT EXISTS idx_items_comments ON items(comments_count);
"""

RE_TOPIC_HREF = re.compile(r"/topics/(\d+)/")

DROP_LABELS = {"定期トピ", "実況・感想", "実況", "感想", "雑談", "相談", "アンケート"}

def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path), timeout=30)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.executescript(DDL)
    con.commit()
    return con

def ensure_columns(con: sqlite3.Connection) -> None:
    cols = {r[1] for r in con.execute("PRAGMA table_info(items)").fetchall()}
    for name in ("post_title", "post_tags", "post_desc"):
        if name not in cols:
            con.execute(f"ALTER TABLE items ADD COLUMN {name} TEXT;")
    con.commit()

def exists_id(con: sqlite3.Connection, tid: str) -> bool:
    return con.execute("SELECT 1 FROM items WHERE id=? LIMIT 1", (tid,)).fetchone() is not None

def upsert(con: sqlite3.Connection, row: Dict[str, Any]) -> None:
    sql = """
    INSERT INTO items (id, check_date, post_date, comments_count, category, title, post_title, post_tags, post_desc)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      check_date=excluded.check_date,
      post_date=excluded.post_date,
      comments_count=excluded.comments_count,
      category=excluded.category,
      title=excluded.title,
      post_title=excluded.post_title,
      post_tags=excluded.post_tags,
      post_desc=excluded.post_desc
    """
    con.execute(sql, (
        row["id"], row["check_date"], row["post_date"], int(row["comments_count"]),
        row["category"], row["title"], row.get("post_title"), row.get("post_tags"), row.get("post_desc")
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

def build_page_url(page_no: int) -> str:
    return f"{BASE_URL}/{page_no}/" + PARAMS

def sanitize_tag(t: str) -> str:
    return (t or "").replace(",", " ").replace("，", " ").replace("\n", " ").strip()

def build_post_fields(title: str, category: str, topic_id: str, check_date: str) -> tuple[str, str, str]:
    raw = (title or "").strip()

    # 【...】抽出 → タグ候補
    bracket_tags = re.findall(r"【([^】]{1,30})】", raw)

    tags = []
    for t in bracket_tags:
        t2 = t.strip()
        if t2 and t2 not in DROP_LABELS:
            tags.append(t2)

    # 【...】を削って本文に
    core = re.sub(r"【[^】]{1,30}】", "", raw).strip()

    # Part / # / 第n弾 / 連番を落とす
    core = re.sub(r"\b(PART|Part|part)\s*\d+\b", "", core).strip()
    core = re.sub(r"[#＃]\s*\d+\b", "", core).strip()
    core = re.sub(r"(第\s*\d+\s*(回|弾))", "", core).strip()
    core = re.sub(r"(パート|Part|PART)\s*\d+\s*$", "", core).strip()

    # 余計な記号の掃除
    core = re.sub(r"\s{2,}", " ", core).strip(" 　-–—_:：")

    post_title = core if core else raw

    # 重複除去（順序維持）＋ sanitize
    seen = set()
    tags2 = []
    for t in tags:
        t2 = sanitize_tag(t)
        if t2 and t2 not in seen:
            tags2.append(t2)
            seen.add(t2)

    post_tags = ",".join(tags2)  # ★カンマ区切り

    url = f"https://girlschannel.net/topics/{topic_id}/"
    post_desc = (
        f"元トピ：{raw}\n"
        f"カテゴリ：{category}\n"
        f"チェック日：{check_date}\n"
        f"URL：{url}\n"
    )
    return post_title, post_tags, post_desc

def main():
    if TARGET_NEW_COUNT <= 0:
        raise SystemExit("TARGET_NEW_COUNT は 1以上にしてください")
    if PAGE_FROM <= 0 or PAGE_TO <= 0 or PAGE_TO < PAGE_FROM:
        raise SystemExit("PAGE_FROM/PAGE_TO の指定が不正です")
    if MIN_COMMENTS < 0:
        raise SystemExit("MIN_COMMENTS は 0以上にしてください")

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
    consecutive_no_save_pages = 0

    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] check_date: {check_date}")
    print(f"[INFO] page_from..to: {PAGE_FROM}..{PAGE_TO}")
    print(f"[INFO] target_save: {TARGET_NEW_COUNT}")
    print(f"[INFO] min_comments: {MIN_COMMENTS}")
    print(f"[INFO] update_existing: {UPDATE_EXISTING}")
    print(f"[INFO] base_url: {BASE_URL}")
    print(f"[INFO] params: {PARAMS}")
    print(f"[INFO] category: {CATEGORY_NAME}")
    print(f"[INFO] early_stop_pages: {EARLY_STOP_PAGES}")
    print(f"[INFO] post_fields: {ENABLE_POST_FIELDS}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context(locale="ja-JP")
        page = context.new_page()

        try:
            pbar = tqdm(total=TARGET_NEW_COUNT, desc="saved")

            for page_no in range(PAGE_FROM, PAGE_TO + 1):
                if saved >= TARGET_NEW_COUNT:
                    break

                pages_done += 1
                url = build_page_url(page_no)

                try:
                    resp = page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                    status = resp.status if resp else None
                    if (not resp) or (status and status >= 400):
                        failed_page += 1
                        print(f"[PAGE_FAIL] page={page_no} status={status} url={url}")
                        time.sleep(SLEEP_SEC)
                        continue
                except PWTimeoutError:
                    failed_page += 1
                    print(f"[PAGE_TIMEOUT] page={page_no} url={url}")
                    time.sleep(SLEEP_SEC)
                    continue

                li_locator = page.locator("xpath=/html/body/div[1]/div[1]/div[1]/ul[2]/li")
                li_count = li_locator.count()
                if li_count == 0:
                    print(f"[NO_ITEMS] page={page_no} url={url}")
                    break

                page_saved = 0

                for idx in range(1, li_count + 1):
                    if saved >= TARGET_NEW_COUNT:
                        break

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

                    # ★フィルタ：一定コメント数未満は保存しない
                    if comments_count < MIN_COMMENTS:
                        skipped_under_min += 1
                        continue

                    # ★既存IDスキップ（ただしUPDATE_EXISTING=Trueなら更新）
                    if (not UPDATE_EXISTING) and exists_id(con, tid):
                        skipped_exists += 1
                        continue

                    post_date = normalize_post_date(post_raw)

                    row: Dict[str, Any] = {
                        "id": tid,
                        "check_date": check_date,
                        "post_date": post_date,
                        "comments_count": comments_count,
                        "category": CATEGORY_NAME,
                        "title": title,
                        "post_title": None,
                        "post_tags": None,
                        "post_desc": None,
                    }

                    if ENABLE_POST_FIELDS:
                        post_title, post_tags, post_desc = build_post_fields(title, CATEGORY_NAME, tid, check_date)
                        row["post_title"] = post_title
                        row["post_tags"] = post_tags
                        row["post_desc"] = post_desc

                    upsert(con, row)
                    con.commit()

                    saved += 1
                    page_saved += 1
                    pbar.update(1)

                    if ECHO_EACH_SAVE:
                        print(
                            f"[OK] page={page_no} li={idx} saved={saved} id={tid} "
                            f"post={post_date} c={comments_count} "
                            f"title={short(title,60)} "
                            f"post_title={short(row['post_title'] or '',40)} "
                            f"tags={row['post_tags'] or ''}"
                        )

                # 早期終了：保存0件ページが続いたら止める
                if page_saved == 0:
                    consecutive_no_save_pages += 1
                    print(
                        f"[NO_SAVE] page={page_no} consecutive={consecutive_no_save_pages} "
                        f"(under_min={skipped_under_min}, exists={skipped_exists}, failed={failed_item})"
                    )
                    if EARLY_STOP_PAGES > 0 and consecutive_no_save_pages >= EARLY_STOP_PAGES:
                        print("[EARLY_STOP] no saved items for consecutive pages -> stop")
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
    print(f"  pages_done={pages_done} range={PAGE_FROM}..{PAGE_TO}")
    print(f"  seen={seen} under_min={skipped_under_min} skipped_exists={skipped_exists} failed_item={failed_item} failed_page={failed_page}")
    if saved == 0:
        print("  [WARN] 保存が0件です。MIN_COMMENTSが高すぎる/ページ範囲が新しすぎる可能性があります。")

if __name__ == "__main__":
    main()
