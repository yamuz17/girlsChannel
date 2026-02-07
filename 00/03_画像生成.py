#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03_画像生成.py（DBキュー方式）【最終版：収まるまで改行 + 収まらない時はタイトル短縮 + bbox実測で保証】

- DB(items) から check_create=STAGE_IN のレコードを1件取得
- folder_name を使って plan_001 配下のフォルダを特定
- text/ranking_ratio_80_plus.ndjson を読み
  1) image/title/title.png
  2) image/comment/{rank}.png
  を生成
- 成功したら check_create を STAGE_OUT にする（= +1）

今回の対策（重要）:
1) 「実描画bbox」基準で横幅判定（右はみ出し問題を潰す）
2) タイトルは「収まるまで改行」(行数を増やしながら最適を探す)
3) それでも収まらない場合は、タイトルを段階的に短縮して再トライ
   （【】タグ削除 / 冗長語削除 / 区切りで後半落とし / 最後は強制トリム）
4) 行bboxのleftが負なら開始X補正（左欠け防止）

前提:
- items テーブルに folder_name カラムがあること
- 02が folder_name を保存していること
"""

from __future__ import annotations

import json
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont


# =========================
# 設定（ここだけ変えればOK）
# =========================
from config import DB_PATH
TABLE_NAME = "items"

BASE_OUTPUT_ROOT = Path(r"/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001")

STAGE_IN  = 2   # 03が処理する段階
STAGE_OUT = 3   # 03完了後の段階

PICK_ORDER = "post_date_desc"  # "post_date_desc" or "comments_desc"
BUSY_TIMEOUT_MS = 60000

# 入力（フォルダ内）
NDJSON_REL = Path("text/ranking_ratio_80_plus.ndjson")

# 出力（フォルダ内）
OUT_TITLE_REL = Path("image/title/title.png")
OUT_COMMENT_REL_DIR = Path("image/comment")

# ======= 画像サイズ（ルール通り） =======
TITLE_W, TITLE_H = 1080, 384
COMMENT_W, COMMENT_H = 1080, 768

# ======= 見た目 =======
PADDING_X = 40
PADDING_Y = 42
BG_ALPHA = 180
RADIUS = 32
VERTICAL_ALIGN = "center"  # "top" or "center"
TARGET_FILL = 0.70

# ======= フォント探索範囲 =======
TITLE_FONT_MAX = 110
TITLE_FONT_MIN = 24   # タイトルの最低を少し下げて保険（必要なら22）

COMMENT_FONT_MAX = 90
COMMENT_FONT_MIN = 24

# タイトルは「収まるまで改行」：行数を増やして対応
TITLE_MAX_LINES_START = 2
TITLE_MAX_LINES_CAP   = 8   # ここまで増やしてOK（見栄え優先なら6）

# コメントは従来通り（max_lines固定）
COMMENT_MAX_LINES = 7

TITLE_LINE_MULT = 1.18    # タイトルは詰め気味（1.25→1.18）
COMMENT_LINE_MULT = 1.35

# 「右端ギリギリ事故」を避けるための安全マージン（px）
SAFE_W_MARGIN = 14        # 10→14（安全側）

# 初期サイズ推定から探索開始を少し上に振る余裕（px）
ESTIMATE_HEADROOM = 10

# タイトル短縮トライ回数
TITLE_SHORTEN_LEVELS = 4

# 最後の砦（強制トリム）で残す文字数
TITLE_HARD_TRIM_CHARS = 26

JP_FONT_CANDIDATES = [
    "/System/Library/Fonts/ヒラギノ角ゴシック W6.ttc",
    "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
    "/System/Library/Fonts/ヒラギノ明朝 ProN W6.ttc",
    "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
]
EMOJI_FONT_PATH = "/System/Library/Fonts/Apple Color Emoji.ttc"
# =========================


def now_jst() -> str:
    return datetime.now(ZoneInfo("Asia/Tokyo")).strftime("%Y-%m-%d %H:%M:%S")


def connect_db() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DBが見つかりません: {DB_PATH}")
    con = sqlite3.connect(str(DB_PATH), timeout=BUSY_TIMEOUT_MS / 1000)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS};")
    return con


def ensure_folder_name_column(con: sqlite3.Connection) -> None:
    cols = {r[1] for r in con.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()}
    if "folder_name" not in cols:
        raise RuntimeError("DBに folder_name カラムがありません。02で追加＆更新してください。")


def pick_one_stage(con: sqlite3.Connection) -> Optional[Tuple[str, str]]:
    order_by = "post_date DESC"
    if PICK_ORDER == "comments_desc":
        order_by = "comments_count DESC, post_date DESC"

    row = con.execute(
        f"""
        SELECT id, folder_name
        FROM {TABLE_NAME}
        WHERE check_create=? AND folder_name IS NOT NULL AND folder_name!=''
        ORDER BY {order_by}
        LIMIT 1
        """,
        (int(STAGE_IN),),
    ).fetchone()
    if not row:
        return None
    return str(row[0]), str(row[1])


def set_stage(con: sqlite3.Connection, tid: str, val: int) -> None:
    con.execute(f"UPDATE {TABLE_NAME} SET check_create=? WHERE id=?", (int(val), tid))
    con.commit()


# ======= フォントキャッシュ =======
_JP_FONT_CACHE: Dict[int, ImageFont.ImageFont] = {}
_EMOJI_FONT_CACHE: Dict[int, Optional[ImageFont.ImageFont]] = {}


def load_font_from_candidates(size: int) -> ImageFont.ImageFont:
    if size in _JP_FONT_CACHE:
        return _JP_FONT_CACHE[size]
    for fp in JP_FONT_CANDIDATES:
        p = Path(fp)
        if p.exists():
            try:
                f = ImageFont.truetype(str(p), size)
                _JP_FONT_CACHE[size] = f
                return f
            except Exception:
                pass
    f = ImageFont.load_default()
    _JP_FONT_CACHE[size] = f
    return f


def load_emoji_font(size: int) -> Optional[ImageFont.ImageFont]:
    if size in _EMOJI_FONT_CACHE:
        return _EMOJI_FONT_CACHE[size]
    p = Path(EMOJI_FONT_PATH)
    if p.exists():
        try:
            f = ImageFont.truetype(str(p), size)
            _EMOJI_FONT_CACHE[size] = f
            return f
        except Exception:
            _EMOJI_FONT_CACHE[size] = None
            return None
    _EMOJI_FONT_CACHE[size] = None
    return None


def read_ndjson(path: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if not path.exists():
        raise FileNotFoundError(f"入力が見つかりません: {path}")

    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError("ndjson が空です")

    first = json.loads(lines[0])
    meta = first.get("meta")
    if not isinstance(meta, dict) or not meta.get("title"):
        raise ValueError("1行目に meta.title がありません（形式が想定と違う）")

    items: List[Dict[str, Any]] = []
    for ln in lines[1:]:
        if not ln.strip():
            continue
        obj = json.loads(ln)
        if "rank" not in obj or "text" not in obj:
            continue
        items.append(obj)

    if not items:
        raise ValueError("rank/text を持つ item が0件です")

    return meta, items


# ======= 絵文字を壊さないための簡易グラフェム分割 =======
def grapheme_clusters(s: str) -> List[str]:
    clusters: List[str] = []
    buf = ""
    prev_was_zwj = False
    for ch in s:
        o = ord(ch)
        is_zwj = (o == 0x200D)
        is_vs16 = (o == 0xFE0F)
        is_skin = (0x1F3FB <= o <= 0x1F3FF)
        is_comb = (0x0300 <= o <= 0x036F)

        if not buf:
            buf = ch
        else:
            if prev_was_zwj or is_zwj or is_vs16 or is_skin or is_comb:
                buf += ch
            else:
                clusters.append(buf)
                buf = ch
        prev_was_zwj = is_zwj
    if buf:
        clusters.append(buf)
    return clusters


def is_emoji_cluster(cluster: str) -> bool:
    for ch in cluster:
        o = ord(ch)
        if (0x1F300 <= o <= 0x1FAFF) or (0x2600 <= o <= 0x27BF) or (0x1F1E6 <= o <= 0x1F1FF):
            return True
    return False


# ======= タイトル短縮 =======
def normalize_title(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    # 先頭の【実況・感想】などを除去（複数回）
    s = re.sub(r"^(【[^】]{1,30}】\s*)+", "", s)
    return s.strip()


def shorten_title_step(s: str, level: int) -> str:
    """
    levelが上がるほど短くする。必ず元より短くなる方向。
    """
    s = normalize_title(s)

    if level >= 1:
        junk = [
            "最終結果", "結果まとめ", "まとめ", "完全版", "速報", "解説", "一覧", "総まとめ",
            "徹底解説", "全まとめ", "完全まとめ", "最終", "決定版", "保存版",
        ]
        for w in junk:
            s = s.replace(w, "")
        s = re.sub(r"\s+", " ", s).strip()

    if level >= 2:
        seps = ["｜", "|", "／", "/", "・", "—", "－", "-", "：", ":"]
        for sep in seps:
            if sep in s:
                parts = [p.strip() for p in s.split(sep) if p.strip()]
                if len(parts) >= 2:
                    s = f"{parts[0]} {parts[1]}"
                elif len(parts) == 1:
                    s = parts[0]
                break
        s = re.sub(r"\s+", " ", s).strip()

    if level >= 3:
        # 文字数ベースの保険（最後の保険）
        if len(s) > TITLE_HARD_TRIM_CHARS:
            s = s[:TITLE_HARD_TRIM_CHARS].rstrip() + "…"

    return s.strip()


def hard_trim_title(s: str) -> str:
    s = normalize_title(s)
    if len(s) > TITLE_HARD_TRIM_CHARS:
        s = s[:TITLE_HARD_TRIM_CHARS].rstrip() + "…"
    return s


# ======= 文字数（グラフェム）から初期フォントサイズを推定 =======
def cluster_em_width_guess(cluster: str) -> float:
    if is_emoji_cluster(cluster):
        return 1.0

    if all(ord(c) < 128 for c in cluster):
        if cluster.isspace():
            return 0.35
        return 0.55

    for c in cluster:
        eaw = unicodedata.east_asian_width(c)
        if eaw in ("F", "W", "A"):
            return 1.0

    return 0.7


def estimate_initial_font_size(text: str, box_w: int, max_lines: int, max_size: int, min_size: int) -> int:
    clusters = [cl for cl in grapheme_clusters(text) if cl != "\n"]
    if not clusters:
        return min_size

    em_sum = 0.0
    for cl in clusters:
        em_sum += cluster_em_width_guess(cl)

    # 行分割ロスを見込んで 0.90
    effective_budget = max(1.0, box_w * max(1, max_lines) * 0.90)
    size = int(effective_budget / max(em_sum, 1e-6))
    size = max(min_size, min(max_size, size))
    return size


# ======= 計測（advance / bbox） =======
def _text_advance_w(draw: ImageDraw.ImageDraw, s: str, font: ImageFont.ImageFont) -> int:
    try:
        return int(draw.textlength(s, font=font))
    except Exception:
        pass
    try:
        return int(font.getlength(s))
    except Exception:
        pass
    bbox = draw.textbbox((0, 0), s, font=font)
    return int(bbox[2] - bbox[0])


def _text_bbox_h(draw: ImageDraw.ImageDraw, s: str, font: ImageFont.ImageFont) -> int:
    bbox = draw.textbbox((0, 0), s, font=font)
    return int(bbox[3] - bbox[1])


def safe_draw_text(draw: ImageDraw.ImageDraw, xy, text, font, fill, embedded_color: bool = False):
    try:
        draw.text(xy, text, font=font, fill=fill, embedded_color=embedded_color)
    except TypeError:
        draw.text(xy, text, font=font, fill=fill)
    except Exception:
        draw.text(xy, text, font=font, fill=fill)


def line_bounds_clusters(draw: ImageDraw.ImageDraw, line: str, font, emoji_font) -> Tuple[int, int]:
    cx = 0
    min_left = 10**9
    max_right = -10**9

    for cl in grapheme_clusters(line):
        use_emoji = bool(emoji_font and is_emoji_cluster(cl))
        f = emoji_font if use_emoji else font

        bbox = draw.textbbox((cx, 0), cl, font=f)
        min_left = min(min_left, int(bbox[0]))
        max_right = max(max_right, int(bbox[2]))

        cx += _text_advance_w(draw, cl, f)

    if max_right < min_left:
        return 0, 0
    return min_left, max_right


def line_width_actual(draw: ImageDraw.ImageDraw, line: str, font, emoji_font) -> int:
    l, r = line_bounds_clusters(draw, line, font, emoji_font)
    return int(r - l)


def draw_text_clusters(draw: ImageDraw.ImageDraw, x: int, y: int, text: str, font, emoji_font, fill):
    cx = x
    for cl in grapheme_clusters(text):
        use_emoji = bool(emoji_font and is_emoji_cluster(cl))
        f = emoji_font if use_emoji else font
        safe_draw_text(draw, (cx, y), cl, f, fill, embedded_color=use_emoji)
        cx += _text_advance_w(draw, cl, f)


def wrap_text_clusters(draw: ImageDraw.ImageDraw, text: str, font, emoji_font, max_w: int) -> List[str]:
    lines: List[str] = []
    buf: List[str] = []

    for cl in grapheme_clusters(text):
        if cl == "\n":
            lines.append("".join(buf))
            buf = []
            continue

        candidate = "".join(buf) + cl
        if buf and line_width_actual(draw, candidate, font, emoji_font) > max_w:
            lines.append("".join(buf))
            buf = [cl]
        else:
            buf.append(cl)

        # 1クラスタがそれ単体で太すぎる場合は破綻回避
        if len(buf) == 1 and line_width_actual(draw, buf[0], font, emoji_font) > max_w:
            lines.append("…")
            buf = []

    if buf:
        lines.append("".join(buf))
    return lines


def ellipsize_line_to_width(draw: ImageDraw.ImageDraw, line: str, font, emoji_font, max_w: int) -> str:
    ell = "…"
    ell_w = line_width_actual(draw, ell, font, emoji_font)

    clusters = grapheme_clusters(line)
    while clusters:
        cur = "".join(clusters)
        if line_width_actual(draw, cur, font, emoji_font) + ell_w <= max_w:
            return cur + ell
        clusters.pop()

    return ell


def ellipsize_lines_to_fit(draw: ImageDraw.ImageDraw, lines: List[str], max_lines: int, font, emoji_font, max_w: int) -> List[str]:
    if len(lines) <= max_lines:
        out: List[str] = []
        for ln in lines:
            if line_width_actual(draw, ln, font, emoji_font) <= max_w:
                out.append(ln)
            else:
                out.append(ellipsize_line_to_width(draw, ln, font, emoji_font, max_w))
        return out

    cut = lines[:max_lines]
    cut[-1] = ellipsize_line_to_width(draw, cut[-1], font, emoji_font, max_w)
    return cut


def compute_line_step(draw: ImageDraw.ImageDraw, size: int, font, line_mult: float) -> int:
    base_h = _text_bbox_h(draw, "あ", font)
    return max(int(size * line_mult), base_h + 6)


def fit_text_autosize(
    draw: ImageDraw.ImageDraw,
    text: str,
    box_w: int,
    box_h: int,
    max_size: int,
    min_size: int,
    max_lines: int,
    line_mult: float,
    target_fill: float,
) -> Tuple[List[str], ImageFont.ImageFont, Optional[ImageFont.ImageFont], int, int]:
    """
    初期サイズ推定→そこから下げながら探索。
    収まり判定は実描画bbox基準。
    """
    best = None
    best_gap = 10**9
    best_underfill = None

    fit_w = max(1, box_w - SAFE_W_MARGIN)

    est = estimate_initial_font_size(text, fit_w, max_lines, max_size, min_size)
    start_size = min(max_size, est + ESTIMATE_HEADROOM)

    for size in range(start_size, min_size - 1, -1):
        font = load_font_from_candidates(size)
        emoji_font = load_emoji_font(size)

        lines = wrap_text_clusters(draw, text, font, emoji_font, max_w=fit_w)
        lines = ellipsize_lines_to_fit(draw, lines, max_lines=max_lines, font=font, emoji_font=emoji_font, max_w=fit_w)

        line_step = compute_line_step(draw, size, font, line_mult)
        total_h = line_step * len(lines)
        if total_h > box_h:
            continue

        max_line_w = 0
        for ln in lines:
            max_line_w = max(max_line_w, line_width_actual(draw, ln, font, emoji_font))
        if max_line_w > fit_w:
            continue

        fill = total_h / max(1, box_h)

        if fill >= target_fill:
            gap = abs(fill - target_fill)
            if gap < best_gap:
                best_gap = gap
                best = (lines, font, emoji_font, line_step, total_h)
        else:
            if best_underfill is None or fill > best_underfill[0]:
                best_underfill = (fill, lines, font, emoji_font, line_step, total_h)

    if best is not None:
        return best

    if best_underfill is not None:
        _, lines, font, emoji_font, line_step, total_h = best_underfill
        return lines, font, emoji_font, line_step, total_h

    # 最後の砦
    font = load_font_from_candidates(min_size)
    emoji_font = load_emoji_font(min_size)
    lines = wrap_text_clusters(draw, text, font, emoji_font, max_w=fit_w)
    lines = ellipsize_lines_to_fit(draw, lines, max_lines=max_lines, font=font, emoji_font=emoji_font, max_w=fit_w)
    line_step = compute_line_step(draw, min_size, font, line_mult)
    total_h = line_step * len(lines)
    return lines, font, emoji_font, line_step, total_h


def fit_text_autosize_flexible_lines(
    draw: ImageDraw.ImageDraw,
    text: str,
    box_w: int,
    box_h: int,
    max_size: int,
    min_size: int,
    max_lines_start: int,
    max_lines_cap: int,
    line_mult: float,
    target_fill: float,
) -> Tuple[List[str], ImageFont.ImageFont, Optional[ImageFont.ImageFont], int, int]:
    """
    行数を増やしながら探索。
    優先度：大きいフォント > 行数少
    """
    best = None
    best_score = None

    for max_lines in range(max_lines_start, max_lines_cap + 1):
        lines, font, emoji_font, line_step, total_h = fit_text_autosize(
            draw, text, box_w, box_h, max_size, min_size, max_lines, line_mult, target_fill
        )
        if total_h > box_h:
            continue

        # 念のため幅チェック
        fit_w = max(1, box_w - SAFE_W_MARGIN)
        max_line_w = 0
        for ln in lines:
            max_line_w = max(max_line_w, line_width_actual(draw, ln, font, emoji_font))
        if max_line_w > fit_w:
            continue

        try:
            size = int(getattr(font, "size", 0)) or 0
        except Exception:
            size = 0

        score = (-size, len(lines))
        if best is None or score < best_score:
            best = (lines, font, emoji_font, line_step, total_h)
            best_score = score

    if best is not None:
        return best

    # 最後の砦
    return fit_text_autosize(
        draw, text, box_w, box_h, max_size, min_size, max_lines_cap, line_mult, target_fill
    )


def calc_start_y(box_h: int, total_h: int) -> int:
    if VERTICAL_ALIGN == "center":
        return max(0, (box_h - total_h) // 2)
    return 0


def _line_start_x_for_bbox(draw: ImageDraw.ImageDraw, base_x: int, line: str, font, emoji_font) -> int:
    left, _ = line_bounds_clusters(draw, line, font, emoji_font)
    if left < 0:
        return base_x - left
    return base_x


def try_fit_title(draw: ImageDraw.ImageDraw, title: str, box_w: int, box_h: int):
    """
    タイトルをフィットできるかを試し、(ok, (lines,font,emoji_font,line_step,total_h)) を返す。
    """
    lines, font, emoji_font, line_step, total_h = fit_text_autosize_flexible_lines(
        draw, title, box_w, box_h,
        TITLE_FONT_MAX, TITLE_FONT_MIN,
        TITLE_MAX_LINES_START, TITLE_MAX_LINES_CAP,
        TITLE_LINE_MULT, TARGET_FILL
    )
    ok = (total_h <= box_h)
    return ok, (lines, font, emoji_font, line_step, total_h)


def make_title_png(title: str, out_path: Path):
    img = Image.new("RGBA", (TITLE_W, TITLE_H), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([0, 0, TITLE_W, TITLE_H], radius=RADIUS, fill=(0, 0, 0, BG_ALPHA))

    box_w = TITLE_W - PADDING_X * 2
    box_h = TITLE_H - PADDING_Y * 2

    # 1) そのまま
    base = normalize_title(title)
    ok, fitted = try_fit_title(d, base, box_w, box_h)
    chosen_title = base

    # 2) ダメなら段階短縮
    if not ok:
        cur = base
        for level in range(1, TITLE_SHORTEN_LEVELS + 1):
            cur2 = shorten_title_step(cur, level)
            if not cur2 or cur2 == cur:
                cur2 = shorten_title_step(base, level)
            cur = cur2
            ok, fitted = try_fit_title(d, cur, box_w, box_h)
            chosen_title = cur
            if ok:
                break

    # 3) それでもダメなら強制トリムで最後の砦
    if not ok:
        chosen_title = hard_trim_title(base)
        ok, fitted = try_fit_title(d, chosen_title, box_w, box_h)

    lines, font, emoji_font, line_step, total_h = fitted

    y = PADDING_Y + calc_start_y(box_h, total_h)
    for line in lines:
        x = _line_start_x_for_bbox(d, PADDING_X, line, font, emoji_font)
        draw_text_clusters(d, x, y, line, font, emoji_font, fill=(255, 255, 255, 255))
        y += line_step

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)


def make_comment_png(rank: int, text: str, out_path: Path):
    img = Image.new("RGBA", (COMMENT_W, COMMENT_H), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([0, 0, COMMENT_W, COMMENT_H], radius=RADIUS, fill=(0, 0, 0, BG_ALPHA))

    line_text = f"{rank}位：{text}".strip()

    box_w = COMMENT_W - PADDING_X * 2
    box_h = COMMENT_H - PADDING_Y * 2

    lines, font, emoji_font, line_step, total_h = fit_text_autosize(
        d, line_text, box_w, box_h,
        COMMENT_FONT_MAX, COMMENT_FONT_MIN,
        COMMENT_MAX_LINES, COMMENT_LINE_MULT, TARGET_FILL
    )

    y = PADDING_Y + calc_start_y(box_h, total_h)
    for line in lines:
        x = _line_start_x_for_bbox(d, PADDING_X, line, font, emoji_font)
        draw_text_clusters(d, x, y, line, font, emoji_font, fill=(255, 255, 255, 255))
        y += line_step

    out_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(out_path)


def main():
    print(f"[INFO] {now_jst()}")
    print(f"[INFO] DB: {DB_PATH}")
    print(f"[INFO] stage_in={STAGE_IN} stage_out={STAGE_OUT}")

    con = connect_db()
    try:
        ensure_folder_name_column(con)

        picked = pick_one_stage(con)
        if not picked:
            print(f"[INFO] check_create={STAGE_IN} が無いので終了")
            return

        tid, folder_name = picked
        base_dir = BASE_OUTPUT_ROOT / folder_name
        ndjson_path = base_dir / NDJSON_REL

        print(f"[PICK] id={tid} folder={folder_name}")
        print(f"[INFO] NDJSON: {ndjson_path}")

        meta, items = read_ndjson(ndjson_path)

        out_title = base_dir / OUT_TITLE_REL
        out_comment_dir = base_dir / OUT_COMMENT_REL_DIR

        title = str(meta.get("title", "")).strip()
        make_title_png(title, out_title)

        created = 0
        for it in items:
            rank = int(it["rank"])
            text = str(it["text"]).strip()
            out = out_comment_dir / f"{rank}.png"
            make_comment_png(rank, text, out)
            created += 1

        print("[OK] created images")
        print(f"  TITLE  : {out_title}")
        print(f"  COMMENT: {out_comment_dir} ({created} files)")

        set_stage(con, tid, STAGE_OUT)
        print(f"[DB] check_create {STAGE_IN} -> {STAGE_OUT} (id={tid})")

    finally:
        con.close()


if __name__ == "__main__":
    main()
