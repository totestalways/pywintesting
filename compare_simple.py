# 01_compare_exact_after_resize.py
# Exact compare after resizing reference to screenshot size.

from utils import imread_any
import cv2
import numpy as np

# ==== CONFIG (edit) ====
REF_PATH = "./refs/sample_ref.png"
SCREENSHOT_PATH = "./artifacts/screenshot.png"  # create with 00_capture_screenshot.py
# =======================

def compare_exact_after_resize(ref_bgr, scr_bgr):
    h, w = scr_bgr.shape[:2]
    ref_r = cv2.resize(ref_bgr, (w, h), interpolation=cv2.INTER_AREA)
    equal = np.array_equal(ref_r, scr_bgr)
    return equal, 1.0 if equal else 0.0

def main():
    ref = imread_any(REF_PATH)
    scr = imread_any(SCREENSHOT_PATH)
    passed, score = compare_exact_after_resize(ref, scr)
    print({"method": "exact_after_resize", "passed": passed, "score": score})

if __name__ == "__main__":
    main()
