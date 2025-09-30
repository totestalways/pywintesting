from utils import imread_any, resize_keep_aspect, ensure_dir
import cv2
import os

# ==== CONFIG (edit) ====
REF_PATH = "./refs/sample_ref.png"        # ideally a sub-image that should appear on screen
SCREENSHOT_PATH = "./artifacts/screenshot.png"
REPORT_DIR = "./artifacts"
TEMPLATE_THRESHOLD = 0.95
# =======================

def template_match(ref_bgr, scr_bgr):
    h_s, w_s = scr_bgr.shape[:2]
    h_r, w_r = ref_bgr.shape[:2]
    ref = ref_bgr
    if w_r > w_s or h_r > h_s:
        ref = resize_keep_aspect(ref_bgr, w_s, h_s)
        h_r, w_r = ref.shape[:2]
    res = cv2.matchTemplate(scr_bgr, ref, cv2.TM_CCOEFF_NORMED)
    _, max_val, _, max_loc = cv2.minMaxLoc(res)
    top_left = max_loc
    bottom_right = (top_left[0] + w_r, top_left[1] + h_r)
    return float(max_val), top_left, bottom_right, ref.shape[:2]

def main():
    ref = imread_any(REF_PATH)
    scr = imread_any(SCREENSHOT_PATH)
    score, tl, br, (h_r, w_r) = template_match(ref, scr)
    ensure_dir(REPORT_DIR)
    vis = scr.copy()
    cv2.rectangle(vis, tl, br, (0, 255, 0), 2)
    out_path = os.path.join(REPORT_DIR, f"template_{os.path.basename(REF_PATH)}.png")
    cv2.imwrite(out_path, vis)
    print({"method": "template", "score": score, "passed": bool(score >= TEMPLATE_THRESHOLD), "match_vis": out_path})

if __name__ == "__main__":
    main()
