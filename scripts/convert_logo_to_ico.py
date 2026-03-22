#!/usr/bin/env python3
"""
将 GuanHeCapital_logo.jpg 转为 ICO / PWA 用 PNG，输出到 web-frontend/public。

- ICO：浏览器标签、favicon 备用
- 192/512 PNG：Web App Manifest（Chrome「添加到主屏幕」）
- 180 PNG：iOS Safari apple-touch-icon

依赖: pip install Pillow
"""
import shutil
from pathlib import Path

from PIL import Image

# 项目根目录（脚本在 scripts/ 下）
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_IMAGE = PROJECT_ROOT / "GuanHeCapital_logo.jpg"
OUT_DIR = PROJECT_ROOT / "web-frontend" / "public"
OUT_ICO = OUT_DIR / "GuanHeCapital.ico"
# 常见 favicon/logo 尺寸
ICO_SIZES = [(16, 16), (32, 32), (48, 48), (256, 256)]
# Chrome PWA / manifest 推荐 PNG；ICO 在 manifest 中支持不一致
PWA_ICONS = (
    (192, "GuanHeCapital_pwa_192.png"),
    (512, "GuanHeCapital_pwa_512.png"),
    (180, "apple-touch-icon.png"),
)


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
    img_ico = img.resize(size_max, Image.Resampling.LANCZOS)
    img_ico.save(OUT_ICO, format="ICO", sizes=ICO_SIZES)
    print(f"已生成: {OUT_ICO}")
    # 多数浏览器会默认请求 /favicon.ico；与 GuanHeCapital.ico 保持一致，避免残留旧品牌
    shutil.copyfile(OUT_ICO, OUT_DIR / "favicon.ico")
    print(f"已生成: {OUT_DIR / 'favicon.ico'}")

    for px, name in PWA_ICONS:
        out = OUT_DIR / name
        scaled = img.resize((px, px), Image.Resampling.LANCZOS)
        scaled.save(out, format="PNG", optimize=True)
        print(f"已生成: {out}")

    return 0


if __name__ == "__main__":
    exit(main())
