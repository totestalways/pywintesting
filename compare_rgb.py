# 03_compare_phash.py
# Perceptual hash similarity (size-agnostic, robust to tiny changes).

from utils import imread_any, to_pil_rgb
import imagehash

# ==== CONFIG (edit) ====
REF_PATH = "./refs/sample_ref.png"
SCREENSHOT_PATH = "./artifacts/screenshot.png"
PHASH_THRESHOLD = 0.90
# =======================

def phash_score(ref_bgr, scr_bgr):
    ref_hash = imagehash.phash(to_pil_rgb(ref_bgr))
    scr_hash = imagehash.phash(to_pil_rgb(scr_bgr))
    dist = ref_hash - scr_hash  # Hamming distance out of 64
    return 1.0 - (dist / 64.0)

def main():
    ref = imread_any(REF_PATH)
    scr = imread_any(SCREENSHOT_PATH)
    score = phash_score(ref, scr)
    print({"method": "phash", "score": score, "passed": bool(score >= PHASH_THRESHOLD)})

if __name__ == "__main__":
    main()
