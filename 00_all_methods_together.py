from utils import imread_any, ensure_dir
import cv2
import os
import numpy as np
from skimage.metrics import structural_similarity
import imagehash
from utils import to_pil_rgb, to_gray, resize_keep_aspect

# ==== CONFIG (edit) ====
REF_PATH = "./refs/sample_ref.png"
SCREENSHOT_PATH = "./artifacts/screenshot.png"
REPORT_DIR = "./artifacts"
THRESHOLDS = {
    "ssim": 0.985,
    "phash": 0.90,
    "template": 0.95,
    "orb_ratio": 0.35,
    "orb_min_inliers": 12,
}
# =======================

def cmp_exact_after_resize(ref_bgr, scr_bgr):
    h, w = scr_bgr.shape[:2]
    ref_r = cv2.resize(ref_bgr, (w, h), interpolation=cv2.INTER_AREA)
    equal = np.array_equal(ref_r, scr_bgr)
    return equal, 1.0 if equal else 0.0

def cmp_ssim_after_resize(ref_bgr, scr_bgr):
    h, w = scr_bgr.shape[:2]
    ref_r = cv2.resize(ref_bgr, (w, h), interpolation=cv2.INTER_AREA)
    score, full = structural_similarity(to_gray(scr_bgr), to_gray(ref_r), full=True)
    diff = (1.0 - full)
    diff_norm = (255 * (diff - diff.min()) / (diff.max() - diff.min() + 1e-12)).astype(np.uint8)
    return float(score), diff_norm

def cmp_phash(ref_bgr, scr_bgr):
    return 1.0 - ((imagehash.phash(to_pil_rgb(ref_bgr)) - imagehash.phash(to_pil_rgb(scr_bgr))) / 64.0)

def cmp_template(ref_bgr, scr_bgr):
    h_s, w_s = scr_bgr.shape[:2]
    h_r, w_r = ref_bgr.shape[:2]
    ref = ref_bgr
    if w_r > w_s or h_r > h_s:
        ref = resize_keep_aspect(ref_bgr, w_s, h_s)
        h_r, w_r = ref.shape[:2]
    res = cv2.matchTemplate(scr_bgr, ref, cv2.TM_CCOEFF_NORMED)
    _, max_val, _, max_loc = cv2.minMaxLoc(res)
    tl = max_loc
    br = (tl[0] + w_r, tl[1] + h_r)
    return float(max_val), tl, br

def cmp_orb(ref_bgr, scr_bgr):
    orb = cv2.ORB_create(nfeatures=1500)
    kp1, des1 = orb.detectAndCompute(to_gray(ref_bgr), None)
    kp2, des2 = orb.detectAndCompute(to_gray(scr_bgr), None)
    if des1 is None or des2 is None or len(kp1) < 10 or len(kp2) < 10:
        return 0.0, 0, 0
    bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=False)
    matches = bf.knnMatch(des1, des2, k=2)
    good = [m for m, n in matches if m.distance < 0.75 * n.distance]
    if len(good) < 8:
        return 0.0, 0, len(good)
    src = np.float32([kp1[m.queryIdx].pt for m in good]).reshape(-1,1,2)
    dst = np.float32([kp2[m.trainIdx].pt for m in good]).reshape(-1,1,2)
    H, mask = cv2.findHomography(src, dst, cv2.RANSAC, 5.0)
    if H is None or mask is None:
        return 0.0, 0, len(good)
    inliers = int(mask.ravel().sum())
    ratio = inliers / max(1, len(good))
    return float(ratio), inliers, len(good)

def main():
    ref = imread_any(REF_PATH)
    scr = imread_any(SCREENSHOT_PATH)
    ensure_dir(REPORT_DIR)

    results = []

    eq_pass, eq_score = cmp_exact_after_resize(ref, scr)
    results.append(("exact", eq_score, eq_pass, {}))

    ssim_score, diff = cmp_ssim_after_resize(ref, scr)
    diff_path = os.path.join(REPORT_DIR, f"diff_ssim_{os.path.basename(REF_PATH)}.png")
    cv2.imwrite(diff_path, cv2.applyColorMap(diff, cv2.COLORMAP_JET))
    results.append(("resize_ssim", ssim_score, ssim_score >= THRESHOLDS["ssim"], {"diff": diff_path}))

    ph = cmp_phash(ref, scr)
    results.append(("phash", ph, ph >= THRESHOLDS["phash"], {}))

    tm, tl, br = cmp_template(ref, scr)
    vis = scr.copy()
    cv2.rectangle(vis, tl, br, (0,255,0), 2)
    tpath = os.path.join(REPORT_DIR, f"template_{os.path.basename(REF_PATH)}.png")
    cv2.imwrite(tpath, vis)
    results.append(("template", tm, tm >= THRESHOLDS["template"], {"match_vis": tpath}))

    ratio, inl, total = cmp_orb(ref, scr)
    results.append(("orb", ratio, (ratio >= THRESHOLDS["orb_ratio"] and inl >= THRESHOLDS["orb_min_inliers"]),
                    {"inliers": inl, "total_good": total}))

    # pick best: prefer passing; tie-break by score; then by method order
    order = {"resize_ssim":0,"exact":1,"template":2,"orb":3,"phash":4}
    passed = [r for r in results if r[2]]
    pool = passed if passed else results
    best = sorted(pool, key=lambda x: (-x[1], order.get(x[0], 99)))[0]

    summary = {
        "per_method": [
            {"method": m, "score": s, "passed": p, "details": d} for (m,s,p,d) in results
        ],
        "best": {"method": best[0], "score": best[1], "passed": best[2], "details": best[3]},
        "thresholds": THRESHOLDS
    }
    print(summary)

if __name__ == "__main__":
    main()
