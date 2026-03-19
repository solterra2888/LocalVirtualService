#!/usr/bin/env python3
"""
临时脚本：将 GuanHeCapital_logo.jpg 转为 ICO 格式，并输出到 web-frontend/public。
依赖: pip install Pillow（或使用项目 .venv: .venv/bin/python scripts/convert_logo_to_ico.py）
使用完毕后可删除此脚本。
"""
import os
from pathlib import Path

from PIL import Image

# 项目根目录（脚本在 scripts/ 下）
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_IMAGE = PROJECT_ROOT / "GuanHeCapital_logo.jpg"
OUT_DIR = PROJECT_ROOT / "web-frontend" / "public"
OUT_ICO = OUT_DIR / "GuanHeCapital.ico"
# 常见 favicon/logo 尺寸
ICO_SIZES = [(16, 16), (32, 32), (48, 48), (256, 256)]


def main():
    if not SRC_IMAGE.exists():
        print(f"错误: 找不到源图 {SRC_IMAGE}")
        return 1
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    img = Image.open(SRC_IMAGE)
    # 转为 RGBA 以便 ICO 支持透明（若原图无透明则全不透明）
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    # 缩放到最大尺寸，Pillow 会按 sizes 生成多尺寸 ICO
    size_max = max(ICO_SIZES, key=lambda s: s[0])
    img = img.resize(size_max, Image.Resampling.LANCZOS)
    img.save(OUT_ICO, format="ICO", sizes=ICO_SIZES)
    print(f"已生成: {OUT_ICO}")
    return 0


if __name__ == "__main__":
    exit(main())
