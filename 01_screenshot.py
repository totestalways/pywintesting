# 00_capture_screenshot.py
# Run this first if you need a fresh screenshot artifact.

from utils import capture_screenshot, ensure_dir
import cv2
import os

# ==== CONFIG (edit) ====
OUT_DIR = "./artifacts"
OUT_PATH = "./artifacts/screenshot.png"
MONITOR = 1
REGION = None        # e.g., (100, 200, 1280, 720) or None for full monitor
# =======================

def main():
    ensure_dir(OUT_DIR)
    img = capture_screenshot(monitor_index=MONITOR, region=REGION)
    cv2.imwrite(OUT_PATH, img)
    h, w = img.shape[:2]
    print({"saved_to": os.path.abspath(OUT_PATH), "size": {"w": w, "h": h}})

if __name__ == "__main__":
    main()
