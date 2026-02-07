#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Tuple, List

from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps


# =========================================================
# 設定（ここだけ変えればOK）
# =========================================================

# ★対象フォルダ（DBの folder_name と同じ）
FOLDER_NAME = "5979943_20260101-163615_漫画家・江口寿史氏、"  # ←ここを書き換え
PLAN_ROOT = Path("/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001")

# main画像（固定）
MAIN_REL = "image/main/1.jpeg"

# 出力先（固定）
OUT_DIR_REL = "image/preview"
OUT_PNG_NAME = "preview.png"

# Shorts縦型サイズ
W = 1080
H = 1920

# 背景：ぼかし + 白黒
BG_BLUR_RADIUS = 18
BG_GRAY = True
BG_DARKEN = 0.12  # 0.0=暗くしない, 0.10〜0.20で前景が立つ

# =========================================================
# タイトル文字 スタイル切替（1〜3）
# =========================================================
TITLE_STYLE_PRESET = 1   # ★ 1 / 2 / 3 を切り替えるだけ
TITLE_ACCENT_WORDS = ["登録"]

TITLE_BOX_POS = "top"
TITLE_BOX_H_RATIO = 0.25
TITLE_PAD_X = 50
TITLE_PAD_Y = 42

TITLE_FONT_SIZE = 82
TITLE_LINE_SPACING = 14
TITLE_STROKE_WIDTH = 6
TITLE_STROKE_FILL = (0, 0, 0, 255)

TITLE_BAND_ENABLE = True
TITLE_BAND_RGBA = (0, 0, 0, 90)

AUTO_SHRINK = True
MIN_FONT_SIZE = 54

# =========================================================
# タイトル文字の取得元（自動）
# =========================================================
TITLE_TXT_REL = "text/title.txt"
META_JSON_REL = "text/meta.json"
NDJSON_SEARCH_DIR_REL = "text"

# =========================================================
# 追加：イントロ preview.mp4 作成（ここまで）
# =========================================================

# ★start mp3 置き場
START_DIR = Path("/Users/yumahama/Library/CloudStorage/GoogleDrive-yuma17.service@gmail.com/マイドライブ/plan_001/start")
START_MP3_NAME = ""  # 空なら最新mp3、固定したいなら "xxx.mp3"

# ★イントロ秒数（0.7秒）
INTRO_SEC = 0.7

# ★イントロ動画を preview フォルダに保存
OUT_MP4_NAME = "preview.mp4"

# 本体と揃えると後で結合が楽
INTRO_FPS = 30
INTRO_AUDIO_SR = 48000
INTRO_AUDIO_CH = 2
INTRO_AUDIO_BITRATE = "192k"


# =========================================================
# スタイル
# =========================================================
def get_title_style(preset: int) -> dict:
    if preset == 1:
        return {"base_fill": (255, 255, 255, 255), "accent_fill": (255, 255, 255, 255)}
    if preset == 2:
        return {"base_fill": (255, 255, 255, 255), "accent_fill": (255, 212, 0, 255)}  # #FFD400
    if preset == 3:
        return {"base_fill": (255, 255, 255, 255), "accent_fill": (0, 229, 255, 255)}  # #00E5FF
    return {"base_fill": (255, 255, 255, 255), "accent_fill": (255, 255, 255, 255)}


# =========================================================
# フォント解決（mac想定）
# =========================================================
def resolve_jp_font_path() -> Path:
    candidates = [
        Path("/System/Library/Fonts/ヒラギノ角ゴシック W6.ttc"),
        Path("/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc"),
        Path("/System/Library/Fonts/Hiragino Sans GB.ttc"),
        Path("/System/Library/Fonts/Hiragino Sans W6.ttc"),
        Path("/System/Library/Fonts/Hiragino Sans W3.ttc"),
        Path("/Library/Fonts/ヒラギノ角ゴシック W6.ttc"),
        Path("/Library/Fonts/Arial Unicode.ttf"),
        Path("/Library/Fonts/NotoSansCJKjp-Bold.otf"),
        Path("/Library/Fonts/NotoSansJP-Bold.otf"),
    ]
    for p in candidates:
        if p.exists():
            return p
    raise FileNotFoundError("日本語フォントが見つかりません。resolve_jp_font_path() の候補に追加してください。")


# =========================================================
# 画像ユーティリティ
# =========================================================
def cover_resize(img: Image.Image, size: Tuple[int, int]) -> Image.Image:
    tw, th = size
    w0, h0 = img.size
    if w0 <= 0 or h0 <= 0:
        raise ValueError("invalid image size")

    src_aspect = w0 / h0
    dst_aspect = tw / th

    if src_aspect > dst_aspect:
        new_h = h0
        new_w = int(h0 * dst_aspect)
        left = (w0 - new_w) // 2
        img = img.crop((left, 0, left + new_w, new_h))
    else:
        new_w = w0
        new_h = int(w0 / dst_aspect)
        top = (h0 - new_h) // 2
        img = img.crop((0, top, new_w, top + new_h))

    return img.resize((tw, th), Image.LANCZOS)


def resize_to_width(img: Image.Image, target_w: int) -> Image.Image:
    w0, h0 = img.size
    if w0 <= 0 or h0 <= 0:
        raise ValueError("invalid image size")
    scale = target_w / w0
    return img.resize((target_w, max(1, int(h0 * scale))), Image.LANCZOS)


def make_bg_blur_gray(main_rgba: Image.Image) -> Image.Image:
    bg = cover_resize(main_rgba, (W, H)).convert("RGBA")
    if BG_BLUR_RADIUS > 0:
        bg = bg.filter(ImageFilter.GaussianBlur(radius=BG_BLUR_RADIUS))

    if BG_GRAY:
        g = ImageOps.grayscale(bg.convert("RGB"))
        bg = Image.merge("RGB", (g, g, g)).convert("RGBA")

    if BG_DARKEN > 0:
        black = Image.new("RGBA", (W, H), (0, 0, 0, 255))
        bg = Image.blend(bg, black, max(0.0, min(1.0, BG_DARKEN)))

    return bg


# =========================================================
# タイトルテキスト取得
# =========================================================
def load_title_text(parent_dir: Path) -> str:
    p1 = parent_dir / TITLE_TXT_REL
    if p1.exists():
        s = p1.read_text(encoding="utf-8", errors="ignore").strip()
        if s:
            return s

    p2 = parent_dir / META_JSON_REL
    if p2.exists():
        try:
            obj = json.loads(p2.read_text(encoding="utf-8", errors="ignore"))
            if isinstance(obj, dict) and str(obj.get("title", "")).strip():
                return str(obj["title"]).strip()
        except Exception:
            pass

    nd_dir = parent_dir / NDJSON_SEARCH_DIR_REL
    if nd_dir.exists():
        for nd in sorted(nd_dir.glob("*.ndjson")):
            try:
                with nd.open("r", encoding="utf-8", errors="ignore") as f:
                    line = f.readline().strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict) and "meta" in obj and isinstance(obj["meta"], dict):
                    t = str(obj["meta"].get("title", "")).strip()
                    if t:
                        return t
            except Exception:
                continue

    name = parent_dir.name
    if "_" in name:
        tail = name.split("_", maxsplit=2)[-1].strip()
        return tail if tail else name
    return name


# =========================================================
# タイトル描画
# =========================================================
def draw_text_with_accent(
    draw: ImageDraw.ImageDraw,
    text: str,
    x: int,
    y: int,
    font: ImageFont.FreeTypeFont,
    base_fill: tuple,
    accent_fill: tuple,
    accent_words: list[str],
    stroke_width: int,
    stroke_fill: tuple,
) -> None:
    if not accent_words:
        draw.text((x, y), text, font=font, fill=base_fill,
                  stroke_width=stroke_width, stroke_fill=stroke_fill)
        return

    cur_x = x
    remain = text

    while remain:
        next_pos = None
        next_word = None
        for w in accent_words:
            if not w:
                continue
            pos = remain.find(w)
            if pos == -1:
                continue
            if (next_pos is None) or (pos < next_pos):
                next_pos = pos
                next_word = w

        if next_pos is None:
            draw.text((cur_x, y), remain, font=font, fill=base_fill,
                      stroke_width=stroke_width, stroke_fill=stroke_fill)
            break

        before = remain[:next_pos]
        if before:
            draw.text((cur_x, y), before, font=font, fill=base_fill,
                      stroke_width=stroke_width, stroke_fill=stroke_fill)
            cur_x += int(draw.textlength(before, font=font))

        word = next_word
        draw.text((cur_x, y), word, font=font, fill=accent_fill,
                  stroke_width=stroke_width, stroke_fill=stroke_fill)
        cur_x += int(draw.textlength(word, font=font))

        remain = remain[next_pos + len(word):]


def wrap_lines(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_w: int) -> List[str]:
    raw_lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not raw_lines:
        return [""]

    lines: List[str] = []
    for raw in raw_lines:
        buf = ""
        for ch in raw:
            trial = buf + ch
            if draw.textlength(trial, font=font) <= max_w:
                buf = trial
                continue
            if buf:
                lines.append(buf)
                buf = ch
            else:
                lines.append(ch)
                buf = ""
        if buf:
            lines.append(buf)
    return lines


def calc_total_text_height(font: ImageFont.FreeTypeFont, n_lines: int) -> int:
    line_h = int(font.size * 1.08)
    if n_lines <= 0:
        return 0
    return n_lines * line_h + (n_lines - 1) * TITLE_LINE_SPACING


# =========================================================
# 追加：イントロmp4作成
# =========================================================
def run(cmd: list[str]) -> None:
    print("[RUN]", " ".join(cmd))
    subprocess.run(cmd, check=True)


def pick_latest_mp3(start_dir: Path, fixed_name: str) -> Path:
    if fixed_name:
        p = start_dir / fixed_name
        if not p.exists():
            raise FileNotFoundError(f"mp3 not found: {p}")
        return p
    mp3s = sorted(start_dir.glob("*.mp3"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not mp3s:
        raise FileNotFoundError(f"no mp3 found in: {start_dir}")
    return mp3s[0]


def make_preview_mp4(preview_png: Path, mp3_path: Path, out_mp4: Path) -> None:
    out_mp4.parent.mkdir(parents=True, exist_ok=True)
    run([
        "ffmpeg", "-y",
        "-loop", "1", "-i", str(preview_png),
        "-i", str(mp3_path),
        "-t", f"{INTRO_SEC}",
        "-vf",
        f"scale={W}:{H}:force_original_aspect_ratio=decrease,"
        f"pad={W}:{H}:(ow-iw)/2:(oh-ih)/2:color=black",
        "-r", str(INTRO_FPS),
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", INTRO_AUDIO_BITRATE,
        "-ar", str(INTRO_AUDIO_SR),
        "-ac", str(INTRO_AUDIO_CH),
        "-shortest",
        str(out_mp4),
    ])


# =========================================================
# 合成（縦プレビュー）
# =========================================================
def build_preview_png(parent_dir: Path) -> Path:
    main_path = parent_dir / MAIN_REL
    if not main_path.exists():
        raise FileNotFoundError(f"main image not found: {main_path}")

    out_dir = parent_dir / OUT_DIR_REL
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / OUT_PNG_NAME

    title_text = load_title_text(parent_dir).strip() or parent_dir.name

    print(f"[INFO] main   = {main_path}")
    print(f"[INFO] title  = {title_text}")
    print(f"[INFO] outpng = {out_path}")

    main_src = Image.open(main_path).convert("RGBA")
    canvas = make_bg_blur_gray(main_src)
    draw = ImageDraw.Draw(canvas)

    main_fit = resize_to_width(main_src, W).convert("RGBA")
    y_main = (H - main_fit.size[1]) // 2
    canvas.alpha_composite(main_fit, (0, y_main))

    box_h = int(H * float(TITLE_BOX_H_RATIO))
    box_y0 = (H - box_h) if (TITLE_BOX_POS == "bottom") else 0

    if TITLE_BAND_ENABLE:
        band = Image.new("RGBA", (W, box_h), TITLE_BAND_RGBA)
        canvas.alpha_composite(band, (0, box_y0))

    font_path = resolve_jp_font_path()
    style = get_title_style(TITLE_STYLE_PRESET)
    base_fill = style["base_fill"]
    accent_fill = style["accent_fill"]
    accent_words = TITLE_ACCENT_WORDS if TITLE_STYLE_PRESET in (2, 3) else []

    font_size = int(TITLE_FONT_SIZE)
    max_w = W - (TITLE_PAD_X * 2)
    max_h = box_h - (TITLE_PAD_Y * 2)

    while True:
        font = ImageFont.truetype(str(font_path), font_size)
        lines = wrap_lines(draw, title_text, font, max_w=max_w)
        text_h = calc_total_text_height(font, len(lines))
        if (not AUTO_SHRINK) or (font_size <= MIN_FONT_SIZE) or (text_h <= max_h and len(lines) <= 4):
            break
        font_size -= 2

    font = ImageFont.truetype(str(font_path), font_size)
    lines = wrap_lines(draw, title_text, font, max_w=max_w)

    total_h = calc_total_text_height(font, len(lines))
    y0 = box_y0 + TITLE_PAD_Y + max(0, (max_h - total_h) // 2)

    line_h = int(font.size * 1.08)
    y = y0
    for ln in lines:
        ln_w = int(draw.textlength(ln, font=font))
        x = TITLE_PAD_X + max(0, (max_w - ln_w) // 2)
        draw_text_with_accent(
            draw=draw,
            text=ln,
            x=x,
            y=y,
            font=font,
            base_fill=base_fill,
            accent_fill=accent_fill,
            accent_words=accent_words,
            stroke_width=TITLE_STROKE_WIDTH,
            stroke_fill=TITLE_STROKE_FILL,
        )
        y += line_h + TITLE_LINE_SPACING

    canvas.save(out_path, format="PNG", optimize=True)
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--folder_name", default=None, help="上部FOLDER_NAMEを上書き")
    ap.add_argument("--parent_dir", default=None, help="folder_nameを使わずフルパス指定")
    ap.add_argument("--preset", type=int, default=None, help="TITLE_STYLE_PRESET を上書き (1-3)")
    ap.add_argument("--intro_sec", type=float, default=None, help="INTRO_SEC を上書き (e.g. 0.7)")
    ap.add_argument("--start_mp3", default=None, help="startのmp3ファイル名を固定（省略なら最新）")
    args = ap.parse_args()

    global TITLE_STYLE_PRESET, INTRO_SEC, START_MP3_NAME
    if args.preset is not None:
        TITLE_STYLE_PRESET = int(args.preset)
    if args.intro_sec is not None:
        INTRO_SEC = float(args.intro_sec)
    if args.start_mp3 is not None:
        START_MP3_NAME = str(args.start_mp3)

    if args.parent_dir:
        parent_dir = Path(args.parent_dir)
    else:
        folder = args.folder_name if args.folder_name else FOLDER_NAME
        if not folder:
            raise SystemExit("FOLDER_NAME が空です。上部のFOLDER_NAMEか --folder_name を指定してください。")
        parent_dir = PLAN_ROOT / folder

    # 1) preview.png
    preview_png = build_preview_png(parent_dir)
    print(f"[OK] preview.png created: {preview_png}")

    # 2) preview.mp4（0.7秒）
    if not START_DIR.exists():
        raise FileNotFoundError(f"START_DIR not found: {START_DIR}")
    mp3_path = pick_latest_mp3(START_DIR, START_MP3_NAME)
    out_mp4 = preview_png.parent / OUT_MP4_NAME

    print(f"[INFO] mp3     = {mp3_path}")
    print(f"[INFO] outmp4  = {out_mp4}")
    print(f"[INFO] sec     = {INTRO_SEC}")
    make_preview_mp4(preview_png=preview_png, mp3_path=mp3_path, out_mp4=out_mp4)
    print(f"[OK] preview.mp4 created: {out_mp4}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
