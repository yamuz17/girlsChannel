#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
02_データ取得.py（DB連携版・keywords_raw保存対応 / env運用 / STA-END方式）

- DB(items) の check_create=STA_02 を1件取得して TARGET_URL を自動決定
- トピックページからタイトル/keywords_raw(meta keywords)/関連キーワード/メイン画像/コメントを取得して保存
- 保存フォルダ名： "{topic_id}_{yyyymmdd-hhmmss}_{タイトル先頭N文字}"
- DBカラム folder_name と keywords_raw に保存
  ★keywords_raw は「JSON配列文字列」で保存（例: ["きっかけ","ゴールイン",...])
- 成功時 check_create を END_02 に更新（失敗時は STA_02 のまま + last_error）

追加仕様（A案）:
- ランキング選定時に NGワードを含むコメントは採用しない（ENABLE_EXCLUDE_BADWORDS）
- 10件作れない場合は「投稿しない」扱いとして check_deploy を +1（check_createは通常通りENDへ）
  → 投稿スクリプト側で COALESCE(check_deploy,0) >= 1 を除外すればOK

注意：
- 02は folder_name がまだ無いので、他スクリプトのように folder_name 条件でピックできない。
  → check_create=STA_02 の id を直接拾う。
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import sqlite3
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from PIL import Image
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

import env_loader


# =========================================================
# .env 読み込み（このスクリプトと同階層の .env）
# =========================================================
_ENV_PATH = env_loader.load_env()  # .env が無ければ例外


# =========================================================
# env helper（02で必要な追加だけ）
# =========================================================
def env_float(name: str, default: float) -> float:
    s = os.environ.get(name, None)
    if s is None:
        return float(default)
    t = str(s).strip().lower()
    if t in ("", "none", "null"):
        return float(default)
    return float(t)


def env_list_csv(name: str, default: List[str]) -> List[str]:
    s = os.environ.get(name, None)
    if s is None:
        return list(default)
    t = str(s).strip()
    if not t:
        return list(default)
    parts = [p.strip() for p in t.split(",")]
    return [p for p in parts if p]


# =========================================================
# 設定（共通envキーを優先）
# =========================================================

# --- 共通：DB / パス ---
DB_PATH = env_loader.env_path("DB_PATH", None)
if DB_PATH is None:
    raise RuntimeError("DB_PATH が .env にありません（必須）")

TABLE_NAME = env_loader.env_str("TABLE_NAME", "items") or "items"

BASE_OUTPUT_ROOT = env_loader.env_path("BASE_OUTPUT_ROOT", None)
if BASE_OUTPUT_ROOT is None:
    raise RuntimeError("BASE_OUTPUT_ROOT が .env にありません（必須）")

# --- 共通：SQLite運用（03などと揃える） ---
BUSY_TIMEOUT_MS = env_loader.env_int("BUSY_TIMEOUT_MS", 60000)

# 例：WAL / NORMAL
SQLITE_JOURNAL_MODE = (env_loader.env_str("SQLITE_JOURNAL_MODE", "WAL") or "WAL").strip()
SQLITE_SYNCHRONOUS = (env_loader.env_str("SQLITE_SYNCHRONOUS", "NORMAL") or "NORMAL").strip()

# --- 共通：DBロック運用 ---
LOCK_RETRY_MAX = env_loader.env_int("LOCK_RETRY_MAX", 25)
LOCK_RETRY_SLEEP_SEC = env_float("LOCK_RETRY_SLEEP_SEC", 0.8)

# --- 共通：ステージ運用（STA/END方式） ---
# STA_02/END_02 があれば優先。無ければ旧 STAGE_IN_02/STAGE_OUT_02 を読む（移行用）
STA_02 = env_loader.env_int("STA_02", -1)
END_02 = env_loader.env_int("END_02", -1)
if STA_02 < 0:
    STA_02 = env_loader.env_int("STAGE_IN_02", 1)
if END_02 < 0:
    END_02 = env_loader.env_int("STAGE_OUT_02", 2)

# --- 共通：ピック順 ---
# 基本は共通の PICK_ORDER を使う（必要なら PICK_ORDER_02 で上書き）
PICK_ORDER_02 = (env_loader.env_str("PICK_ORDER_02", "") or "").strip()
if not PICK_ORDER_02:
    PICK_ORDER_02 = (env_loader.env_str("PICK_ORDER", "post_date_desc") or "post_date_desc").strip()

# --- 02固有：取得系 ---
MAX_COMMENTS_TO_FETCH = env_loader.env_int("MAX_COMMENTS_TO_FETCH", 75)
MAX_CONSECUTIVE_MISSES = env_loader.env_int("MAX_CONSECUTIVE_MISSES", 20)

HEADLESS_MODE = env_loader.env_bool("HEADLESS_MODE", True)
WAIT_TIMEOUT = env_loader.env_int("WAIT_TIMEOUT_MS", 45000)  # ms
REQUEST_INTERVAL_MS = env_loader.env_int("REQUEST_INTERVAL_MS", 300)
USER_AGENT = env_loader.env_str(
    "USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
) or ""

SAVE_DEBUG_ON_PARSE_FAIL = env_loader.env_bool("SAVE_DEBUG_ON_PARSE_FAIL", True)
ENABLE_RELATED_KEYWORDS = env_loader.env_bool("ENABLE_RELATED_KEYWORDS", True)

# フォルダ名タイトルの最大文字数
FOLDER_TITLE_MAX_CHARS = env_loader.env_int("FOLDER_TITLE_MAX_CHARS", 10)

# 投稿しない判定（運用でON/OFFしたくなるので env化）
ENABLE_DEPLOY_SKIP_IF_SHORTAGE = env_loader.env_bool("ENABLE_DEPLOY_SKIP_IF_SHORTAGE", True)
REQUIRE_TOTAL_N = env_loader.env_int("REQUIRE_TOTAL_N", 10)
REQUIRE_RATIO_N = env_loader.env_int("REQUIRE_RATIO_N", 10)

# NGワード
ENABLE_EXCLUDE_BADWORDS = env_loader.env_bool("ENABLE_EXCLUDE_BADWORDS", True)
BADWORDS_DEFAULT = [
    "殺", "死", "亡",
    "自殺", "他殺", "事故死",
    "ころす", "殺す", "死ね", "氏ね",
    "しぬ", "ﾀﾋ", "タヒ", "レイプ", "ﾚｲﾌﾟ", "売春", "朝鮮",
    "ガルちゃん", "ｶﾞﾙちゃん","ガル民", "ｶﾞﾙ民",
]
BADWORDS = env_list_csv("BADWORDS_CSV", BADWORDS_DEFAULT)
BADWORDS_NORMALIZE = env_loader.env_bool("BADWORDS_NORMALIZE", True)

# ランキング（ここはコード調整寄り）
TOP_N_TOTAL = 10
TOP_N_RATIO = 10
RANKING_EXTRA_CANDIDATES = 20
RATIO_THRESHOLD = 0.8
MIN_TOTAL_VOTES = 5

# メイン画像（固定）
MAIN_IMAGE_XPATH = "/html/body/div[1]/div[1]/div/div[1]/img"
MAIN_DIR_NAME = "main"
MAIN_IMAGE_FILENAME = "1.jpeg"

# ブラウザ
VIEWPORT_SIZE = {"width": 1280, "height": 1024}

# 仕様①：主語なし AND 10文字以内を除外（固定）
ENABLE_EXCLUDE_SHORT_SUBJECTLESS = True
SHORT_SUBJECTLESS_MAX_LEN = 10
JP_PARTICLES = ("は", "が", "を", "に", "で", "と", "へ", "から", "まで", "より", "って")
INTERJECTION_ONLY_RE = re.compile(r"^[ぁ-んァ-ンー〜～…!！?？、。,\s]+$")

# 仕様③④：候補だけ要約→まだ長ければ除外
ENABLE_SUMMARY_FOR_RANKING = True
MAX_TEXT_CHARS_ALLOWED = 70
SUMMARY_MAX_SENTENCES = 2

# NDJSON meta
JSON_ORDER = "bottom_to_top"
IMAGE_PLACEHOLDER_DIR = "images"
IMAGE_PLACEHOLDER_EXT = ".jpg"

# 関連キーワード
RELATED_KEYWORDS_CSS = "ul.keywords li a"

# ===================== 正規表現 =====================
TITLE_BRACKET_RE = re.compile(r"【[^】]*】")
EMOJI_RE = re.compile(
    "["
    "\U0001F1E6-\U0001F1FF"
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\u2600-\u27BF"
    "\uFE0F"
    "\u200D"
    "]",
    flags=re.UNICODE
)


# ===================== DBユーティリティ =====================
def now_jst() -> str:
    return datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")


def connect_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"DBが見つかりません: {db_path}")

    con = sqlite3.connect(str(db_path), timeout=BUSY_TIMEOUT_MS / 1000)
    con.row_factory = sqlite3.Row

    # 共通envに寄せる
    con.execute(f"PRAGMA journal_mode={SQLITE_JOURNAL_MODE};")
    con.execute(f"PRAGMA synchronous={SQLITE_SYNCHRONOUS};")
    con.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS};")
    return con


def ensure_columns(con: sqlite3.Connection) -> None:
    cols_lower = {str(r[1]).lower() for r in con.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()}

    need = {
        "check_create": "INTEGER NOT NULL DEFAULT 0",
        "check_deploy": "INTEGER NOT NULL DEFAULT 0",
        "folder_name": "TEXT",
        "keywords_raw": "TEXT",
        "last_error": "TEXT",
        "updated_at": "TEXT",
    }

    for name, ddl in need.items():
        if name.lower() not in cols_lower:
            con.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {name} {ddl};")
            cols_lower.add(name.lower())

    con.commit()


def pick_one_stage_id(con: sqlite3.Connection, stage: int) -> Optional[str]:
    # 02は folder_name 未生成なので folder_name 条件は付けない
    order_sql = "id DESC"
    if PICK_ORDER_02 == "post_date_desc":
        order_sql = "post_date DESC, id DESC"
    elif PICK_ORDER_02 == "comments_desc":
        # itemsに comment_count が無い場合あり得るので注意（あるなら使える）
        order_sql = "comment_count DESC, post_date DESC, id DESC"

    row = con.execute(
        f"SELECT id FROM {TABLE_NAME} WHERE check_create=? ORDER BY {order_sql} LIMIT 1",
        (int(stage),)
    ).fetchone()
    return str(row["id"]) if row else None


def update_stage_success(con: sqlite3.Connection, tid: str, folder_name: str, keywords_raw: str) -> None:
    for attempt in range(1, LOCK_RETRY_MAX + 1):
        try:
            con.execute("BEGIN IMMEDIATE;")
            cur = con.execute(
                f"""
                UPDATE {TABLE_NAME}
                   SET check_create=?,
                       folder_name=?,
                       keywords_raw=?,
                       last_error=NULL,
                       updated_at=?
                 WHERE id=? AND check_create=?
                """,
                (int(END_02), folder_name, (keywords_raw or "").strip(), now_jst(), tid, int(STA_02))
            )
            con.execute("COMMIT;")
            if cur.rowcount == 0:
                raise RuntimeError(f"update_success rowcount=0: id={tid} check_createがSTA_02({STA_02})ではない可能性")
            return
        except sqlite3.OperationalError as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            if "locked" in str(e).lower():
                print(f"[LOCK] retry {attempt}/{LOCK_RETRY_MAX} on update_success")
                time.sleep(LOCK_RETRY_SLEEP_SEC)
                continue
            raise
    raise sqlite3.OperationalError("database is locked (retry exceeded) on update_success")


def update_stage_error(con: sqlite3.Connection, tid: str, err: str) -> None:
    for attempt in range(1, LOCK_RETRY_MAX + 1):
        try:
            con.execute("BEGIN IMMEDIATE;")
            con.execute(
                f"""
                UPDATE {TABLE_NAME}
                   SET last_error=?,
                       updated_at=?
                 WHERE id=?
                """,
                (err[:2000], now_jst(), tid)
            )
            con.execute("COMMIT;")
            return
        except sqlite3.OperationalError as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            if "locked" in str(e).lower():
                print(f"[LOCK] retry {attempt}/{LOCK_RETRY_MAX} on update_error")
                time.sleep(LOCK_RETRY_SLEEP_SEC)
                continue
            raise
    raise sqlite3.OperationalError("database is locked (retry exceeded) on update_error")


def increment_check_deploy(con: sqlite3.Connection, tid: str, reason: str) -> None:
    for attempt in range(1, LOCK_RETRY_MAX + 1):
        try:
            con.execute("BEGIN IMMEDIATE;")
            con.execute(
                f"""
                UPDATE {TABLE_NAME}
                   SET check_deploy = COALESCE(check_deploy,0) + 1,
                       last_error   = ?,
                       updated_at   = ?
                 WHERE id=?
                """,
                (f"deploy_skip: {reason}"[:2000], now_jst(), tid)
            )
            con.execute("COMMIT;")
            return
        except sqlite3.OperationalError as e:
            try:
                con.execute("ROLLBACK;")
            except Exception:
                pass
            if "locked" in str(e).lower():
                print(f"[LOCK] retry {attempt}/{LOCK_RETRY_MAX} on increment_check_deploy")
                time.sleep(LOCK_RETRY_SLEEP_SEC)
                continue
            raise
    raise sqlite3.OperationalError("database is locked (retry exceeded) on increment_check_deploy")


# ===================== 文字処理 =====================
def extract_topic_id(url: str) -> str:
    m = re.search(r"/topics/(\d+)/", url)
    if not m:
        raise ValueError(f"URLからtopic_idを抽出できません: {url}")
    return m.group(1)


def remove_emojis(text: str) -> str:
    if not text:
        return ""
    return EMOJI_RE.sub("", text)


def clean_title(title: str) -> str:
    t = (title or "").strip()
    t = TITLE_BRACKET_RE.sub("", t)
    t = remove_emojis(t)
    t = re.sub(r"\s+", " ", t).strip()
    return t if t else "タイトルなし"


def sanitize_for_folder_name(name: str, max_len: int = 60) -> str:
    s = (name or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[\/\\:\*\?\"<>\|\n\r\t]", "_", s)
    s = s.strip(" .")
    if not s:
        s = "タイトルなし"
    return s[:max_len]


def title_for_folder(title_cleaned: str) -> str:
    t = (title_cleaned or "").strip() or "タイトルなし"
    t = t[:FOLDER_TITLE_MAX_CHARS]
    return sanitize_for_folder_name(t, max_len=FOLDER_TITLE_MAX_CHARS)


def remove_quote_anchors(text: str) -> str:
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out_lines = []
    for ln in lines:
        ln2 = re.sub(r"^\s*[>＞]{1,2}\s*[0-9０-９]+(?:\s*-\s*[0-9０-９]+)?\s*", "", ln)
        if ln2.strip():
            out_lines.append(ln2)
    return "\n".join(out_lines).strip()


def is_subjectless_like(text: str) -> bool:
    t = re.sub(r"\s+", " ", text.strip())
    if not t:
        return True
    if any(p in t for p in JP_PARTICLES):
        return False
    if re.search(r"[一-龥A-Za-z0-9]", t):
        return False
    if INTERJECTION_ONLY_RE.match(t):
        return True
    return True


def should_exclude_short_subjectless(text: str) -> bool:
    if not ENABLE_EXCLUDE_SHORT_SUBJECTLESS:
        return False
    t = re.sub(r"\s+", " ", text.strip())
    return (len(t) <= SHORT_SUBJECTLESS_MAX_LEN) and is_subjectless_like(t)


def summarize_by_sentences(text: str, max_sentences: int = 2) -> str:
    t = re.sub(r"\s+", " ", text.strip())
    if max_sentences <= 0:
        return t
    parts = re.split(r"(。)", t)
    if len(parts) < 2:
        return t
    rebuilt = ""
    cnt = 0
    for i in range(0, len(parts) - 1, 2):
        rebuilt += parts[i] + parts[i + 1]
        cnt += 1
        if cnt >= max_sentences:
            break
    return rebuilt.strip()


def parse_jp_number(text: str) -> int:
    if not text:
        return 0
    t = text.strip().replace(",", "")
    t = t.translate(str.maketrans("０１２３４５６７８９．", "0123456789."))
    m = re.match(r"^(\d+(?:\.\d+)?)(万|千)?$", t)
    if not m:
        m2 = re.search(r"\d+", t)
        return int(m2.group(0)) if m2 else 0
    num = float(m.group(1))
    unit = m.group(2)
    if unit == "万":
        return int(num * 10000)
    if unit == "千":
        return int(num * 1000)
    return int(num)


def placeholder_image_path(rank: int) -> str:
    return f"{IMAGE_PLACEHOLDER_DIR}/{rank:03d}{IMAGE_PLACEHOLDER_EXT}"


def save_bytes_as_jpeg(body: bytes, out_path: Path) -> None:
    img = Image.open(BytesIO(body))
    img = img.convert("RGB")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path, format="JPEG", quality=92, optimize=True)


def clean_keyword_tag(text: str) -> str:
    t = (text or "").strip()
    t = t.lstrip("#").strip()
    t = remove_emojis(t)
    t = t.replace(",", " ").replace("，", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def meta_keywords_to_json_array_string(meta_content: str) -> str:
    s = (meta_content or "").strip()
    if not s:
        return ""
    parts = re.split(r"[,\n\r\t　]+", s)

    out: List[str] = []
    seen = set()
    for p in parts:
        t = clean_keyword_tag(p)
        if not t:
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)

    if not out:
        return ""
    return json.dumps(out, ensure_ascii=False)


def normalize_for_badword_check(s: str) -> str:
    t = (s or "")
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    t = re.sub(r"\s+", "", t)
    return t.lower()


def contains_badword(text: str) -> bool:
    if not ENABLE_EXCLUDE_BADWORDS:
        return False
    t = normalize_for_badword_check(text) if BADWORDS_NORMALIZE else (text or "")
    for w in BADWORDS:
        ww = normalize_for_badword_check(w) if BADWORDS_NORMALIZE else w
        if ww and ww in t:
            return True
    return False


# ===================== スクレイピング =====================
async def extract_related_keywords(page) -> List[str]:
    loc = page.locator(RELATED_KEYWORDS_CSS)
    cnt = await loc.count()
    if cnt == 0:
        return []
    texts = await loc.all_inner_texts()
    out: List[str] = []
    for s in texts:
        t = clean_keyword_tag(s)
        if t:
            out.append(t)
    seen = set()
    uniq: List[str] = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


async def extract_keywords_raw_from_meta(page) -> str:
    for sel in (
        'meta[name="keywords"]',
        'meta[name="Keywords"]',
        'meta[name="keyword"]',
        'meta[name="Keyword"]',
    ):
        loc = page.locator(sel).first
        if await loc.count() > 0:
            content = await loc.get_attribute("content")
            if content and content.strip():
                return meta_keywords_to_json_array_string(content.strip())

    try:
        content = await page.evaluate(
            "() => document.querySelector('meta[name=\"keywords\"],meta[name=\"Keywords\"],meta[name=\"keyword\"],meta[name=\"Keyword\"]')?.getAttribute('content') || ''"
        )
        if isinstance(content, str) and content.strip():
            return meta_keywords_to_json_array_string(content.strip())
    except Exception:
        pass

    return ""


async def scrape(url: str) -> Tuple[str, str, str, Path, Path, Optional[Path], str, List[str], List[Dict]]:
    print(f"[INFO] TARGET_URL: {url}")

    topic_id = extract_topic_id(url)
    thread_title = "タイトルなし"
    keywords_raw_json = ""
    related_keywords: List[str] = []
    comments: List[Dict] = []
    run_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS_MODE)
        context = await browser.new_context(
            viewport=VIEWPORT_SIZE,
            user_agent=USER_AGENT,
            locale="ja-JP",
        )
        page = await context.new_page()
        page.set_default_timeout(WAIT_TIMEOUT)

        try:
            print("1. トピックページへアクセスしてタイトル取得...")
            await page.goto(url, wait_until="domcontentloaded")

            title_loc = page.locator("h1#topic-title-h1").first
            if await title_loc.count() == 0:
                title_loc = page.locator("h1").first

            if await title_loc.count() > 0:
                try:
                    thread_title = (await title_loc.inner_text()).strip()
                except Exception:
                    pass

            thread_title = clean_title(thread_title)

            folder_title = title_for_folder(thread_title)
            folder_name = f"{topic_id}_{run_stamp}_{folder_title}"

            base_dir = BASE_OUTPUT_ROOT / folder_name
            text_dir = base_dir / "text"
            image_dir = base_dir / "image"
            debug_dir = text_dir / "debug"

            text_dir.mkdir(parents=True, exist_ok=True)
            image_dir.mkdir(parents=True, exist_ok=True)
            if SAVE_DEBUG_ON_PARSE_FAIL:
                debug_dir.mkdir(parents=True, exist_ok=True)

            main_dir = image_dir / MAIN_DIR_NAME
            main_dir.mkdir(parents=True, exist_ok=True)

            print(f"   -> タイトル: {thread_title}")
            print(f"   -> folder_name: {folder_name}")
            print(f"   -> 出力先: {base_dir}")

            # keywords_raw(meta keywords) を JSON配列文字列で保存
            try:
                keywords_raw_json = await extract_keywords_raw_from_meta(page)
            except Exception:
                keywords_raw_json = ""

            (text_dir / "keywords_raw.txt").write_text(keywords_raw_json or "", encoding="utf-8")
            if keywords_raw_json:
                print(f"   -> keywords_raw(meta/json): {keywords_raw_json[:200]}" + ("..." if len(keywords_raw_json) > 200 else ""))
            else:
                print("   -> keywords_raw(meta): 取得なし")

            # 関連キーワード
            if ENABLE_RELATED_KEYWORDS:
                try:
                    related_keywords = await extract_related_keywords(page)
                except Exception:
                    related_keywords = []

                kw_path = text_dir / "related_keywords.txt"
                with kw_path.open("w", encoding="utf-8") as f:
                    for k in related_keywords:
                        f.write(k + "\n")

                if related_keywords:
                    print(f"   -> 関連キーワード: {', '.join(related_keywords[:10])}" + (" ..." if len(related_keywords) > 10 else ""))
                else:
                    print("   -> 関連キーワード: 取得なし")

            print("2. メイン画像取得...")
            main_img_path: Optional[Path] = None
            try:
                img_loc = page.locator(f"xpath={MAIN_IMAGE_XPATH}").first
                if await img_loc.count() > 0:
                    src = (
                        await img_loc.get_attribute("src")
                        or await img_loc.get_attribute("data-src")
                        or await img_loc.get_attribute("data-original")
                    )
                    if src:
                        img_url = urljoin(url, src)
                        resp = await context.request.get(img_url)
                        if resp.ok:
                            body = await resp.body()
                            main_img_path = main_dir / MAIN_IMAGE_FILENAME
                            save_bytes_as_jpeg(body, main_img_path)
            except Exception:
                main_img_path = None

            if main_img_path:
                print(f"   -> メイン画像保存: {main_img_path}")
            else:
                print("   -> メイン画像: 取得できませんでした（スキップ）")

            print("3. コメントページを順番に取得...")
            consecutive_misses = 0
            plus_line_re = re.compile(r"^[\+＋]\s*([0-9０-９,]+(?:\.[0-9０-９]+)?(?:万|千)?)\s*$", re.M)
            minus_line_re = re.compile(r"^[\-−]\s*([0-9０-９,]+(?:\.[0-9０-９]+)?(?:万|千)?)\s*$", re.M)

            for n in range(1, MAX_COMMENTS_TO_FETCH + 1):
                comment_url = f"https://girlschannel.net/comment/{topic_id}/{n}/"
                print(f"\rコメント取得中: {n} / {MAX_COMMENTS_TO_FETCH}", end="", flush=True)

                resp = await page.goto(comment_url, wait_until="domcontentloaded")
                status = resp.status if resp is not None else None

                if status in (404, 410):
                    consecutive_misses += 1
                    if consecutive_misses >= MAX_CONSECUTIVE_MISSES:
                        break
                    await page.wait_for_timeout(REQUEST_INTERVAL_MS)
                    continue

                try:
                    body_text = await page.locator("body").inner_text()
                except PlaywrightTimeoutError:
                    consecutive_misses += 1
                    await page.wait_for_timeout(REQUEST_INTERVAL_MS)
                    continue

                body_text = body_text.replace("\r\n", "\n")

                id_line = re.search(rf"^{n}\.\s*匿名.*$", body_text, flags=re.M)
                if not id_line:
                    consecutive_misses += 1
                    if SAVE_DEBUG_ON_PARSE_FAIL:
                        (text_dir / "debug" / f"parse_fail_{n}.txt").write_text(body_text, encoding="utf-8")
                    await page.wait_for_timeout(REQUEST_INTERVAL_MS)
                    continue

                tail = body_text[id_line.end():]

                plus_m = plus_line_re.search(tail)
                if not plus_m:
                    consecutive_misses += 1
                    if SAVE_DEBUG_ON_PARSE_FAIL:
                        (text_dir / "debug" / f"parse_fail_{n}.txt").write_text(body_text, encoding="utf-8")
                    await page.wait_for_timeout(REQUEST_INTERVAL_MS)
                    continue

                after_plus = tail[plus_m.end():]
                minus_m = minus_line_re.search(after_plus)

                plus_text = plus_m.group(1)
                minus_text = minus_m.group(1) if minus_m else "0"

                comment_body_raw = tail[:plus_m.start()].strip()
                comment_body_raw = remove_quote_anchors(comment_body_raw)
                comment_body_raw = remove_emojis(comment_body_raw).strip()

                # 仕様①（短い主語なし）
                if should_exclude_short_subjectless(comment_body_raw):
                    consecutive_misses = 0
                    await page.wait_for_timeout(REQUEST_INTERVAL_MS)
                    continue

                plus_count = parse_jp_number(plus_text)
                minus_count = parse_jp_number(minus_text)
                total_count = plus_count + minus_count
                ratio = (plus_count / total_count) if total_count > 0 else 0.0

                comments.append(
                    {
                        "id": str(n),
                        "body_raw": comment_body_raw,
                        "body": comment_body_raw,
                        "plus": plus_count,
                        "minus": minus_count,
                        "total": total_count,
                        "ratio": ratio,
                    }
                )

                consecutive_misses = 0
                await page.wait_for_timeout(REQUEST_INTERVAL_MS)

            print("\rコメント取得完了。                        ")
            print(f"[INFO] comments_fetched: {len(comments)}")

            return thread_title, run_stamp, folder_name, text_dir, image_dir, main_img_path, keywords_raw_json, related_keywords, comments

        finally:
            await browser.close()


# ===================== ランキング作成（A案） =====================
def build_ranked_selection(ranked_all: List[Dict], want_n: int, extra_candidates: int) -> List[Dict]:
    result: List[Dict] = []
    n_all = len(ranked_all)
    if n_all == 0:
        return result

    scan = max(want_n + max(extra_candidates, 0), want_n)
    idx = 0
    while len(result) < want_n and idx < n_all:
        end = min(scan, n_all)

        for c in ranked_all[idx:end]:
            txt = c.get("body_raw", "") or c.get("body", "") or ""
            txt = re.sub(r"\s+", " ", txt).strip()

            if contains_badword(txt):
                continue

            if ENABLE_SUMMARY_FOR_RANKING and len(txt) > MAX_TEXT_CHARS_ALLOWED:
                txt = summarize_by_sentences(txt, max_sentences=SUMMARY_MAX_SENTENCES)

            if len(txt) > MAX_TEXT_CHARS_ALLOWED:
                continue

            cc = dict(c)
            cc["body"] = txt
            result.append(cc)

            if len(result) >= want_n:
                break

        idx = end
        scan = min(scan + max(extra_candidates, 1), n_all)
        if idx < n_all and scan == idx:
            scan = min(idx + 1, n_all)

    return result


def write_txt_ranking(path: Path, title: str, header: str, rows: List[Dict], include_ratio: bool = False) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(f"【スレッドタイトル】: {title}\n\n--- {header} ---\n\n")
        for i, c in enumerate(rows):
            if include_ratio:
                ratio_percent = c["ratio"] * 100
                f.write(f"【順位: {i+1}】 (高評価率: {ratio_percent:.1f}%, +{c['plus']}/-{c['minus']})\n")
            else:
                f.write(f"【順位: {i+1}】 (合計: {c['total']}, +{c['plus']}/-{c['minus']})\n")
            f.write(f"{c['id']}: {c['body']}\n\n")


def write_ndjson_ranking(
    path: Path,
    title: str,
    created_stamp: str,
    order: str,
    rows: List[Dict],
    points_key: str,
    tags: Optional[List[str]] = None,
) -> None:
    with path.open("w", encoding="utf-8") as f:
        meta = {
            "meta": {
                "title": f"{title}",
                "order": order,
                "created": created_stamp,
                "tags": tags or [],
            }
        }
        f.write(json.dumps(meta, ensure_ascii=False) + "\n")

        n = len(rows)

        def row_to_obj(rank: int, c: Dict) -> Dict:
            points = int(c.get(points_key, 0))
            delta = int(c.get("plus", 0)) - int(c.get("minus", 0))
            return {
                "rank": rank,
                "points": points,
                "delta": delta,
                "text": c.get("body", ""),
                "image": placeholder_image_path(rank),
            }

        if order == "bottom_to_top":
            for i in range(n, 0, -1):
                c = rows[i - 1]
                f.write(json.dumps(row_to_obj(i, c), ensure_ascii=False) + "\n")
        else:
            for i, c in enumerate(rows, start=1):
                f.write(json.dumps(row_to_obj(i, c), ensure_ascii=False) + "\n")


def analyze_and_save(
    title: str,
    created_stamp: str,
    comments: List[Dict],
    text_dir: Path,
    tags: Optional[List[str]] = None
) -> Tuple[int, int]:
    text_dir.mkdir(parents=True, exist_ok=True)

    ranked_total_all = sorted(comments, key=lambda x: x["total"], reverse=True)
    ranking_total = build_ranked_selection(ranked_total_all, TOP_N_TOTAL, RANKING_EXTRA_CANDIDATES)

    txt_total = text_dir / "ranking_total.txt"
    ndjson_total = text_dir / "ranking_total.ndjson"
    write_txt_ranking(txt_total, title, "総合評価数ランキング", ranking_total, include_ratio=False)
    write_ndjson_ranking(ndjson_total, title, created_stamp, JSON_ORDER, ranking_total, points_key="total", tags=tags)

    print(f"[SAVE] {txt_total} ({len(ranking_total)}件)")
    print(f"[SAVE] {ndjson_total}")

    filtered_ratio = [c for c in comments if c["ratio"] >= RATIO_THRESHOLD and c["total"] > MIN_TOTAL_VOTES]
    ranked_ratio_all = sorted(filtered_ratio, key=lambda x: x["plus"], reverse=True)
    ranking_ratio = build_ranked_selection(ranked_ratio_all, TOP_N_RATIO, RANKING_EXTRA_CANDIDATES)

    txt_ratio = text_dir / "ranking_ratio_80_plus.txt"
    ndjson_ratio = text_dir / "ranking_ratio_80_plus.ndjson"
    write_txt_ranking(txt_ratio, title, f"高評価率 ({int(RATIO_THRESHOLD*100)}%以上) ランキング", ranking_ratio, include_ratio=True)
    write_ndjson_ranking(ndjson_ratio, title, created_stamp, JSON_ORDER, ranking_ratio, points_key="plus", tags=tags)

    print(f"[SAVE] {txt_ratio} ({len(ranking_ratio)}件)")
    print(f"[SAVE] {ndjson_ratio}")

    return (len(ranking_total), len(ranking_ratio))


# ===================== メイン =====================
def main() -> None:
    print(f"[INFO] {now_jst()}")
    print(f"[INFO] env: {_ENV_PATH}")
    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] table: {TABLE_NAME}")
    print(f"[INFO] stage: STA_02={STA_02} -> END_02={END_02}")
    print(f"[INFO] sqlite: journal_mode={SQLITE_JOURNAL_MODE} synchronous={SQLITE_SYNCHRONOUS} busy_timeout_ms={BUSY_TIMEOUT_MS}")
    print(f"[INFO] output_root: {BASE_OUTPUT_ROOT}")
    print(f"[INFO] headless={HEADLESS_MODE} max_comments={MAX_COMMENTS_TO_FETCH} pick_order_02={PICK_ORDER_02}")

    con = connect_db(DB_PATH)
    try:
        ensure_columns(con)

        tid = pick_one_stage_id(con, STA_02)
        if not tid:
            print(f"[INFO] check_create={STA_02} のIDがありません（終了）")
            return

        target_url = f"https://girlschannel.net/topics/{tid}/"
        print(f"[INFO] picked id={tid} -> TARGET_URL={target_url}")

        try:
            title, run_stamp, folder_name, text_dir, image_dir, main_img, keywords_raw_json, related_tags, comments_data = asyncio.run(
                scrape(target_url)
            )

            if not comments_data:
                raise RuntimeError("コメントが取得できませんでした（0件）")

            total_n, ratio_n = analyze_and_save(title, run_stamp, comments_data, text_dir=text_dir, tags=related_tags)

            if ENABLE_DEPLOY_SKIP_IF_SHORTAGE:
                shortage = []
                if total_n < REQUIRE_TOTAL_N:
                    shortage.append(f"total={total_n}/{REQUIRE_TOTAL_N}")
                if ratio_n < REQUIRE_RATIO_N:
                    shortage.append(f"ratio={ratio_n}/{REQUIRE_RATIO_N}")
                if shortage:
                    reason = "ranking_shortage " + " ".join(shortage)
                    print(f"[INFO] deploy_skip -> {reason}")
                    increment_check_deploy(con, tid, reason)

            update_stage_success(con, tid, folder_name, keywords_raw_json)

            print("\n[OK] 02完了")
            print(f"  id={tid}")
            print(f"  folder_name={folder_name}")
            print(f"  text_dir={text_dir}")
            print(f"  image_dir={image_dir}")
            print(f"  keywords_raw(json)={keywords_raw_json[:200] + ('...' if len(keywords_raw_json) > 200 else '') if keywords_raw_json else '(なし)'}")
            if ENABLE_RELATED_KEYWORDS:
                print(f"  related_keywords={', '.join(related_tags) if related_tags else '(なし)'}")
            if main_img:
                print(f"  main_img={main_img}")
            print(f"  ranking_counts: total={total_n} ratio={ratio_n}")
            if ENABLE_EXCLUDE_BADWORDS:
                print(f"  badwords_enabled: {len(BADWORDS)} words")

        except Exception as e:
            update_stage_error(con, tid, f"{type(e).__name__}: {e}")
            print(f"\n[FAIL] 02失敗 id={tid} err={type(e).__name__}: {e}")
            raise

    finally:
        con.close()


if __name__ == "__main__":
    main()
