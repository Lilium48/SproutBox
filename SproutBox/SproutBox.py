#!/usr/bin/env python3
"""
SproutVideo → Box (Developer Token edition) — resilient uploader
- Uses Box Developer Token (no refresh; expires ~60 minutes).
- Supports simple upload and chunked upload with SHA‑1 commit.
- Adds robust retry/backoff for Box throttling (429/503/etc.), honors Retry‑After,
  handles 202 on commit, and soft rate‑limits part uploads for stability.
"""

import os
import re
import time
import random
import hashlib
import tempfile
from typing import Optional

import requests
import certifi
from dotenv import load_dotenv

from boxsdk import Client
from boxsdk.exception import BoxAPIException, BoxOAuthException
from boxsdk.network.default_network import DefaultNetwork
from boxsdk.session.session import AuthorizedSession
from boxsdk.auth import DeveloperTokenAuth

load_dotenv()

# ---- SproutVideo ----
SPROUT_BASE_URL = "https://api.sproutvideo.com/v1"
SPROUT_API_KEY = os.getenv("SPROUT_API_KEY")
SPROUT_FOLDER_ID = os.getenv("SPROUT_FOLDER_ID")

# ---- Box ----
BOX_PARENT_FOLDER_ID = os.getenv("BOX_PARENT_FOLDER_ID")
BOX_ACCESS_TOKEN = os.getenv("BOX_ACCESS_TOKEN")  # Developer Token

# ---- Misc / Tunables ----
# Network timeouts (seconds)
TIMEOUT = int(os.getenv("TIMEOUT", "300"))                # Box SDK request timeout
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "300"))  # Sprout download timeout
UAGENT = {"User-Agent": "sprout-to-box/1.1"}

# Control how many videos this run processes (0 = no cap)
MAX_VIDEOS = int(os.getenv("MAX_VIDEOS", "0"))

# Soft cap on part upload request rate (requests per second). 2–3 is conservative.
UPLOAD_RPS_SOFT_CAP = float(os.getenv("UPLOAD_RPS_SOFT_CAP", "2.0"))

# Optional tiny pause between videos (seconds); helps smooth bursts
INTER_VIDEO_PAUSE_SEC = float(os.getenv("INTER_VIDEO_PAUSE_SEC", "0"))

# Use simple upload up to this size (MB). 150 MB reduces session churn & rate pressure.
SIMPLE_UPLOAD_THRESHOLD_MB = int(os.getenv("SIMPLE_UPLOAD_THRESHOLD_MB", "150"))

# Backoff cap for Retry-After-less cases (seconds)
BACKOFF_CAP_SECONDS = int(os.getenv("BACKOFF_CAP_SECONDS", "900"))

# ---------------------------------------------------------------------------
# Utilities & Sprout API helpers
# ---------------------------------------------------------------------------

def must(name, value):
    if not value:
        raise SystemExit(f"[FATAL] Missing required env var: {name}")

def sprout_get(path, params=None):
    url = f"{SPROUT_BASE_URL}{path}"
    headers = {**UAGENT, "SproutVideo-Api-Key": SPROUT_API_KEY}
    r = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=TIMEOUT,
        verify=(CA_BUNDLE if VERIFY_SSL else False),
    )
    r.raise_for_status()
    return r.json()

def iter_videos(folder_id, per_page=100):
    page = 1
    while True:
        j = sprout_get("/videos", {"folder_id": folder_id, "page": page, "per_page": per_page})
        items = j.get("videos") or j.get("items") or []
        if not items:
            break
        for v in items:
            yield v
        page += 1

def list_subfolders(folder_id):
    # try /folders/{id} (nested) then /folders?parent_id=
    try:
        j = sprout_get(f"/folders/{folder_id}")
        subs = j.get("folders") or []
        if subs:
            return subs
    except Exception:
        pass
    try:
        j = sprout_get("/folders", {"parent_id": folder_id, "per_page": 100})
        return j.get("folders") or j.get("items") or []
    except Exception:
        return []

# ---------- Asset helpers (robust MP4 discovery) ----------

def _ext_from_url(url: str) -> str:
    if not isinstance(url, str) or not url:
        return ""
    base = url.split("?", 1)[0].split("#", 1)[0]
    _, ext = os.path.splitext(base)
    return (ext or "").lower().lstrip(".")

def _infer_height_from_url(url: str) -> int:
    if not isinstance(url, str):
        return 0
    # match 720p, 1080p, etc.
    m = re.search(r'(\d{3,4})p(?=\.mp4\b)', url, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # match /240.mp4, _360.mp4, -480.mp4
    m = re.search(r'[/_\-](\d{3,4})(?=\.mp4\b)', url)
    return int(m.group(1)) if m else 0

def _norm_container(container: str, url: str) -> str:
    c = (container or "").lower()
    if not c or c in ("application/octet-stream",):
        c = _ext_from_url(url)
    if c in ("video/mp4", "mp4"):
        return "mp4"
    if c in ("image/jpeg", "image/jpg", "jpg", "jpeg", "png", "gif", "image"):
        return "image"
    if c in ("m3u8", "application/x-mpegurl", "vnd.apple.mpegurl", "hls"):
        return "hls"
    return c or ""

def _push_asset(acc, url, container=None, height=0, bitrate=0):
    if isinstance(url, str) and url:
        cont = _norm_container(container, url)
        h = int(height or 0)
        if not h and cont == "mp4":
            h = _infer_height_from_url(url)
        acc.append({
            "url": url,
            "container": cont,
            "height": h,
            "bitrate": int(bitrate or 0),
        })

def _collect_assets(node, acc):
    """Recursively collect any URLs in the 'assets' structure."""
    if node is None:
        return
    if isinstance(node, str):
        _push_asset(acc, node, _ext_from_url(node), _infer_height_from_url(node), 0)
    elif isinstance(node, list):
        for v in node:
            _collect_assets(v, acc)
    elif isinstance(node, dict):
        # Standard asset object: {url, container, height, ...}
        if "url" in node and isinstance(node.get("url"), str):
            _push_asset(acc, node["url"], node.get("container"), node.get("height"), node.get("bitrate"))
        else:
            # Nested dict (e.g., {"videos": {"240p": ".../240.mp4"}, "thumbnails": [...]})
            for v in node.values():
                _collect_assets(v, acc)

def pick_best_mp4(video_id):
    """Return (download_url, title, height) for best MP4."""
    j = sprout_get(f"/videos/{video_id}")
    raw = j.get("assets") or {}
    assets = []
    _collect_assets(raw, assets)

    # Keep only real MP4s
    def is_mp4(a):
        cont = a.get("container")
        if cont == "mp4":
            return True
        u = (a.get("url") or "").lower().split("?", 1)[0]
        return u.endswith(".mp4")

    mp4s = [a for a in assets if is_mp4(a)]
    if not mp4s:
        kinds = sorted(set(a.get("container") for a in assets if a.get("container")))
        sample = str(raw)[:200]
        raise RuntimeError(
            f"No MP4 assets found for video {video_id}. "
            f"Saw containers: {kinds or ['(none)']}. Sample: {sample}"
        )

    # Prefer higher resolution, then higher bitrate
    mp4s.sort(key=lambda a: (a.get("height", 0), a.get("bitrate", 0)))
    best = mp4s[-1]
    title = j.get("title") or j.get("name") or video_id
    return best["url"], title, int(best.get("height") or 0)

# ---------------------------------------------------------

def safe_filename(name):
    base = "".join(c if c.isalnum() or c in "._- " else "_" for c in (name or "video")).strip()
    if not base.lower().endswith(".mp4"):
        base += ".mp4"
    return base

def download_to_temp(url, filename_hint):
    path = os.path.join(tempfile.gettempdir(), filename_hint)
    # Include Sprout API key in case the file endpoint enforces it
    headers = {**UAGENT, "SproutVideo-Api-Key": SPROUT_API_KEY}
    with requests.get(
        url,
        stream=True,
        headers=headers,
        timeout=DOWNLOAD_TIMEOUT,
        verify=(CA_BUNDLE if VERIFY_SSL else False),
    ) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    f.write(chunk)
    return path

# -------- DEV TOKEN auth: use a callback instead of access_token= --------

def _get_dev_token():
    tok = BOX_ACCESS_TOKEN
    if not tok:
        raise SystemExit("[FATAL] BOX_ACCESS_TOKEN is not set")
    return tok

def box_client():
    """Build a Box client using Developer Token (via callback)."""
    network = DefaultNetwork()
    dev_auth = DeveloperTokenAuth(get_new_token_callback=_get_dev_token)
    session = AuthorizedSession(
        dev_auth,
        network_layer=network,
        default_headers=UAGENT,
        default_network_request_kwargs={
            "timeout": TIMEOUT,
            "verify": (CA_BUNDLE if VERIFY_SSL else False),
        },
    )
    return Client(dev_auth, session=session)

# ---------------------------------------------------------------------------
# Backoff / Throttle helpers for Box uploads
# ---------------------------------------------------------------------------

def _retry_after_seconds_from_headers(headers) -> Optional[int]:
    if not headers:
        return None
    for k in ("Retry-After", "retry-after", "RETRY-AFTER"):
        v = headers.get(k)
        if v is None:
            continue
        try:
            # Header can be integer seconds or HTTP-date; we handle integer seconds.
            return max(0, int(float(v)))
        except Exception:
            pass
    return None

def _sleep_for_retry(attempt: int, retry_after: Optional[int], cap_seconds: int = BACKOFF_CAP_SECONDS) -> float:
    """Sleep according to Retry-After if present; else exponential backoff with jitter."""
    if retry_after is not None:
        delay = min(retry_after, cap_seconds)
    else:
        delay = min(cap_seconds, (2 ** min(attempt, 6)) + random.random())  # 1–64s + jitter
    time.sleep(delay)
    return delay

_last_part_call_t = 0.0
def _throttle_part_upload():
    """Ensure we don't exceed our soft cap of part upload requests/sec."""
    global _last_part_call_t
    if UPLOAD_RPS_SOFT_CAP <= 0:
        return
    min_gap = 1.0 / UPLOAD_RPS_SOFT_CAP
    now = time.monotonic()
    wait = (_last_part_call_t + min_gap) - now
    if wait > 0:
        time.sleep(wait)
    _last_part_call_t = time.monotonic()

def _headers_from_box_exc(e: BoxAPIException):
    # Prefer network_response.headers; fall back to e.headers
    hdrs = {}
    try:
        if getattr(e, "network_response", None) and e.network_response and e.network_response.headers:
            hdrs.update(e.network_response.headers)
    except Exception:
        pass
    try:
        if getattr(e, "headers", None) and e.headers:
            hdrs.update(e.headers)
    except Exception:
        pass
    return hdrs

# ---------------------------------------------------------------------------
# Box folder & upload logic with robust retries
# ---------------------------------------------------------------------------

def ensure_box_subfolder(client, parent_id, name):
    for it in client.folder(parent_id).get_items(limit=1000):
        if it.type == "folder" and it.name == name:
            return it.id
    try:
        return client.folder(parent_id).create_subfolder(name).id
    except BoxAPIException as e:
        if e.status == 409:  # already exists
            for it in client.folder(parent_id).get_items(limit=1000):
                if it.type == "folder" and it.name == name:
                    return it.id
        raise

def upload_file(client, local_path, parent_id):
    """Use simple upload for small files; chunked for large files with robust retries."""
    name = os.path.basename(local_path)
    size = os.path.getsize(local_path)

    SIMPLE_THRESHOLD = SIMPLE_UPLOAD_THRESHOLD_MB * 1024 * 1024

    if size <= SIMPLE_THRESHOLD:
        attempt = 0
        while True:
            attempt += 1
            try:
                client.folder(parent_id).upload(local_path, file_name=name)
                return name
            except BoxAPIException as e:
                # Handle name conflict quickly; otherwise apply backoff on 429/5xx
                if e.status == 409:
                    stem, ext = os.path.splitext(name)
                    alt = f"{stem}__dup{int(time.time())}{ext}"
                    client.folder(parent_id).upload(local_path, file_name=alt)
                    return alt
                if e.status in (429, 500, 502, 503, 504):
                    ra = _retry_after_seconds_from_headers(_headers_from_box_exc(e))
                    delay = _sleep_for_retry(attempt, ra)
                    print(f"• simple upload backoff {delay:.1f}s on {e.status} (req {e.request_id})")
                    continue
                raise
    else:
        # Chunked upload with retries
        # 1) Create session
        attempt = 0
        while True:
            attempt += 1
            try:
                session = client.folder(parent_id).create_upload_session(file_size=size, file_name=name)
                break
            except BoxAPIException as e:
                if e.status in (429, 500, 502, 503, 504):
                    ra = _retry_after_seconds_from_headers(_headers_from_box_exc(e))
                    delay = _sleep_for_retry(attempt, ra)
                    print(f"• create_upload_session backoff {delay:.1f}s on {e.status} (req {e.request_id})")
                    continue
                raise

        part_size = session.part_size
        parts = []
        sha1 = hashlib.sha1()
        try:
            with open(local_path, "rb") as f:
                offset = 0
                while offset < size:
                    chunk = f.read(part_size)
                    if not chunk:
                        break
                    sha1.update(chunk)

                    # 2) Upload this part with retry & throttle
                    up_attempt = 0
                    while True:
                        up_attempt += 1
                        try:
                            _throttle_part_upload()
                            part = session.upload_part_bytes(chunk, offset, size)
                            parts.append(part)
                            break
                        except BoxAPIException as e:
                            if e.status in (429, 500, 502, 503, 504):
                                ra = _retry_after_seconds_from_headers(_headers_from_box_exc(e))
                                delay = _sleep_for_retry(up_attempt, ra)
                                print(f"• upload_part backoff {delay:.1f}s on {e.status} (req {e.request_id}, off {offset})")
                                continue
                            raise
                    offset += len(chunk)

            content_sha1 = sha1.digest()

            # 3) Commit with retry (handle 202 + Retry-After and 409 name conflicts)
            commit_attempt = 0
            while True:
                commit_attempt += 1
                try:
                    file_info = session.commit(content_sha1, parts=parts)
                    return file_info.name
                except BoxAPIException as e:
                    if e.status in (202, 409, 429, 500, 502, 503, 504):
                        if e.status == 409:
                            # Name reserved or conflict – pick a unique name and retry commit
                            stem, ext = os.path.splitext(name)
                            alt = f"{stem}__dup{int(time.time())}{ext}"
                            try:
                                file_info = session.commit(content_sha1, parts=parts, file_attributes={"name": alt})
                                return file_info.name
                            except BoxAPIException as ee:
                                # If still not ready / throttled, fall through to backoff with ee
                                e = ee
                        ra = _retry_after_seconds_from_headers(_headers_from_box_exc(e))
                        delay = _sleep_for_retry(commit_attempt, ra)
                        print(f"• commit backoff {delay:.1f}s on {e.status} (req {e.request_id})")
                        continue
                    raise
        except Exception:
            # Optional: abort session to free server resources on unrecoverable failure
            try:
                session.abort()
            except Exception:
                pass
            raise

def process_folder_recursive(client, sprout_folder_id, box_parent_id, depth=0):
    indent = "  " * depth
    print(f"{indent}→ Folder {sprout_folder_id} → Box {box_parent_id}")

    processed = 0

    # videos in folder
    for v in iter_videos(sprout_folder_id):
        if MAX_VIDEOS and processed >= MAX_VIDEOS:
            print(f"{indent}  • Reached MAX_VIDEOS={MAX_VIDEOS}; stopping this run.")
            return
        vid = v.get("id") or v.get("video_id")
        tmp = None
        try:
            best_url, title, height = pick_best_mp4(vid)
            fname = safe_filename(title)
            print(f"{indent}  - {title} ({vid}) → chosen {height or 'unknown'}p")
            tmp = download_to_temp(best_url, fname)
            uploaded_name = upload_file(client, tmp, box_parent_id)
            print(f"{indent}    ✓ Uploaded {uploaded_name}")
        except Exception as e:
            print(f"{indent}  ! Video {vid} failed: {e}")
        finally:
            if tmp and os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass
        processed += 1

        # Optional small pause to smooth request bursts across files
        if INTER_VIDEO_PAUSE_SEC > 0:
            time.sleep(INTER_VIDEO_PAUSE_SEC)

    # subfolders
    for sub in list_subfolders(sprout_folder_id):
        sub_id = sub.get("id")
        sub_name = (sub.get("name") or sub_id or "").strip() or "untitled"
        try:
            sub_box_id = ensure_box_subfolder(client, box_parent_id, sub_name)
            process_folder_recursive(client, sub_id, sub_box_id, depth + 1)
        except Exception as e:
            print(f"{indent}  ! Subfolder '{sub_name}' failed: {e}")

# ---------------------------------------------------------------------------

if __name__ == "__main__":
    must("SPROUT_API_KEY", SPROUT_API_KEY)
    must("SPROUT_FOLDER_ID", SPROUT_FOLDER_ID)
    must("BOX_PARENT_FOLDER_ID", BOX_PARENT_FOLDER_ID)
    must("BOX_ACCESS_TOKEN", BOX_ACCESS_TOKEN)  # dev token

    client = box_client()

    # Confirm Box identity & folder access early (helps catch expired dev token)
    try:
        me = client.user().get()
        tgt = client.folder(BOX_PARENT_FOLDER_ID).get()
        print(f"Box user: {me.login} ({me.id})  | Target folder: {tgt.name} ({tgt.id})")
    except (BoxAPIException, BoxOAuthException) as e:
        raise SystemExit(
            f"[FATAL] Box access failed: {e}\n"
            "Hint: Developer Tokens expire ~60 minutes. Generate a new one and update BOX_ACCESS_TOKEN."
        )

    process_folder_recursive(client, SPROUT_FOLDER_ID, BOX_PARENT_FOLDER_ID)
    print("Done.")
