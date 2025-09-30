from utils import imread_any, to_gray, ensure_dir
from skimage.metrics import structural_similarity
import cv2
import numpy as np
import os

# ==== CONFIG (edit) ====
REF_PATH = "./refs/sample_ref.png"
SCREENSHOT_PATH = "./artifacts/screenshot.png"
REPORT_DIR = "./artifacts"
SSIM_THRESHOLD = 0.985
# =======================

def resize_ssim(ref_bgr, scr_bgr):
    h, w = scr_bgr.shape[:2]
    ref_r = cv2.resize(ref_bgr, (w, h), interpolation=cv2.INTER_AREA)
    score, full = structural_similarity(to_gray(scr_bgr), to_gray(ref_r), full=True)
    diff = (1.0 - full)
    diff_norm = (255 * (diff - diff.min()) / (diff.max() - diff.min() + 1e-12)).astype(np.uint8)
    return float(score), diff_norm

def main():
    ref = imread_any(REF_PATH)
    scr = imread_any(SCREENSHOT_PATH)
    score, diff = resize_ssim(ref, scr)
    ensure_dir(REPORT_DIR)
    diff_bgr = cv2.applyColorMap(diff, cv2.COLORMAP_JET)
    out_path = os.path.join(REPORT_DIR, f"diff_ssim_{os.path.basename(REF_PATH)}.png")
    cv2.imwrite(out_path, diff_bgr)
    print({"method": "resize_ssim", "score": score, "passed": bool(score >= SSIM_THRESHOLD), "diff": out_path})

if __name__ == "__main__":
    main()
