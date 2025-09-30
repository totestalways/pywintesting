import os
from typing import Optional, Tuple, List
import cv2
import numpy as np
from PIL import Image
from mss import mss

IMG_EXTS = {".png", ".jpg", ".jpeg", ".bmp", ".webp"}

def ensure_dir(p: Optional[str]) -> None:
    if p and p.strip():
        os.makedirs(p, exist_ok=True)

def imread_any(path: str) -> np.ndarray:
    """Read image as BGR uint8; convert gray->BGR, drop alpha if present."""
    img = cv2.imread(path, cv2.IMREAD_UNCHANGED)
    if img is None:
        raise FileNotFoundError(f"Failed to read image: {path}")
    if img.ndim == 2:
        img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
    if img.shape[2] == 4:
        img = img[:, :, :3]
    return img

def to_gray(img_bgr: np.ndarray) -> np.ndarray:
    return cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)

def to_pil_rgb(img_bgr: np.ndarray) -> Image.Image:
    return Image.fromarray(cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB))

def resize_keep_aspect(img: np.ndarray, max_w: int, max_h: int) -> np.ndarray:
    h, w = img.shape[:2]
    scale = min(max_w / w, max_h / h)
    if scale >= 1.0:
        return img.copy()
    new_w, new_h = int(w * scale), int(h * scale)
    return cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)

def capture_screenshot(monitor_index: int = 1,
                       region: Optional[Tuple[int, int, int, int]] = None) -> np.ndarray:
    """
    Capture screen using mss. Returns BGR image (uint8).
    monitor_index: 1 = primary monitor per mss.
    region: (x,y,w,h) relative to selected monitor; None = full monitor.
    """
    with mss() as sct:
        if monitor_index < 1 or monitor_index > len(sct.monitors) - 1:
            monitor_index = 1
        mon = sct.monitors[monitor_index]
        if region:
            x, y, w, h = region
            bbox = {"left": mon["left"] + x, "top": mon["top"] + y, "width": w, "height": h}
        else:
            bbox = {"left": mon["left"], "top": mon["top"], "width": mon["width"], "height": mon["height"]}
        shot = sct.grab(bbox)
        img = np.array(shot)[:, :, :3]  # BGRA -> BGR
        return img

def list_ref_images(ref_dir: str) -> List[str]:
    imgs = []
    for name in os.listdir(ref_dir):
        ext = os.path.splitext(name)[1].lower()
        if ext in IMG_EXTS:
            imgs.append(os.path.join(ref_dir, name))
    if not imgs:
        raise FileNotFoundError(f"No reference images found in {ref_dir}")
    return sorted(imgs)
