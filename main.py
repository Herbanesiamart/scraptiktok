import asyncio
import os
import uuid
import zipfile
import json
import shutil
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks, Form
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse
import httpx

app = FastAPI()

APP_PASSWORD = os.environ.get("APP_PASSWORD", "adsy2024")
TIKWM_BASE = "https://www.tikwm.com/api"

# In-memory job store
jobs: dict = {}


@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    with open("index.html", encoding="utf-8") as f:
        return f.read()


@app.post("/api/start")
async def start_download(
    background_tasks: BackgroundTasks,
    password: str = Form(...),
    username: str = Form(...),
    start: int = Form(1),
    end: int = Form(10),
    order: str = Form("newest"),
):
    if password != APP_PASSWORD:
        raise HTTPException(status_code=401, detail="Password salah")

    if start < 1 or end < start:
        raise HTTPException(status_code=400, detail="Range tidak valid")

    if end - start >= 50:
        raise HTTPException(status_code=400, detail="Maksimal 50 video sekali download")

    username = username.strip().lstrip("@")
    if not username:
        raise HTTPException(status_code=400, detail="Username tidak boleh kosong")

    order = order if order in ("newest", "oldest") else "newest"
    job_id = str(uuid.uuid4())

    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "total": end - start + 1,
        "logs": [],
        "zip_path": None,
        "username": username,
    }

    background_tasks.add_task(run_download, job_id, username, start, end, order)

    return {"job_id": job_id}


def add_log(job_id: str, msg: str):
    if job_id in jobs:
        jobs[job_id]["logs"].append(msg)


async def fetch_user_videos(username: str, needed: int, log_fn=None) -> list:
    """Ambil daftar video dari profil TikTok via TikWM API."""
    all_videos = []
    cursor = 0

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        while len(all_videos) < needed:
            # TikWM pakai POST dengan form data
            resp = await client.post(
                f"{TIKWM_BASE}/user/posts",
                data={"unique_id": username, "count": 20, "cursor": cursor},
                headers={"User-Agent": "Mozilla/5.0"},
            )

            if resp.status_code != 200:
                raise Exception(f"TikWM API HTTP {resp.status_code}: {resp.text[:300]}")

            try:
                data = resp.json()
            except Exception:
                raise Exception(f"TikWM response bukan JSON: {resp.text[:300]}")

            if log_fn:
                log_fn(f"📡 TikWM response code: {data.get('code')} | total video: {data.get('data', {}).get('total', '?')}")

            if data.get("code") != 0:
                raise Exception(f"TikWM error: {data.get('msg', 'unknown')} (code {data.get('code')})")

            videos = data.get("data", {}).get("videos", [])
            if not videos:
                break

            all_videos.extend(videos)

            if not data["data"].get("hasMore"):
                break

            cursor = data["data"].get("cursor", 0)

    return all_videos


async def run_download(job_id: str, username: str, start: int, end: int, order: str = "newest"):
    output_dir = Path(f"/tmp/tktk_{job_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        jobs[job_id]["status"] = "downloading"
        add_log(job_id, f"🔍 Mengambil daftar video @{username}...")

        all_videos = await fetch_user_videos(username, end, log_fn=lambda m: add_log(job_id, m))

        if not all_videos:
            jobs[job_id]["status"] = "error"
            add_log(job_id, "❌ Akun tidak ditemukan atau tidak ada video.")
            return

        # Slice sesuai range (1-indexed)
        selected = all_videos[start - 1 : end]

        if order == "oldest":
            selected = list(reversed(selected))

        total = len(selected)
        jobs[job_id]["total"] = total

        if total == 0:
            jobs[job_id]["status"] = "error"
            add_log(job_id, f"❌ Tidak ada video di range {start}–{end}. Akun hanya punya {len(all_videos)} video.")
            return

        add_log(job_id, f"✅ Ditemukan {len(all_videos)} video. Mengunduh {total} video (#{start}–#{start + total - 1})...")

        downloaded = 0

        async with httpx.AsyncClient(timeout=60) as client:
            for i, video in enumerate(selected):
                video_id = video.get("video_id") or video.get("id", f"video_{i}")
                title = video.get("title", "")[:40].strip() or video_id

                # Ambil URL video tanpa watermark (HD dulu, fallback ke play)
                video_url = video.get("hdplay") or video.get("play")

                if not video_url:
                    add_log(job_id, f"⚠️ Video {i+1}/{total} tidak ada URL, skip.")
                    continue

                add_log(job_id, f"⬇️  Video {i+1}/{total}: {title}")

                try:
                    resp = await client.get(video_url, follow_redirects=True)
                    resp.raise_for_status()

                    filename = f"{str(i+1).zfill(5)}_{video_id}.mp4"
                    filepath = output_dir / filename

                    with open(filepath, "wb") as f:
                        f.write(resp.content)

                    downloaded += 1
                    jobs[job_id]["progress"] = downloaded
                    add_log(job_id, f"✅ Video {i+1}/{total} selesai")

                except Exception as e:
                    add_log(job_id, f"⚠️ Video {i+1}/{total} gagal: {str(e)}")

        files = list(output_dir.iterdir())
        if not files:
            jobs[job_id]["status"] = "error"
            add_log(job_id, "❌ Semua video gagal didownload.")
            return

        add_log(job_id, f"📦 Membuat ZIP dari {len(files)} video...")

        zip_path = Path(f"/tmp/tktk_{job_id}.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(files):
                zf.write(f, f.name)

        shutil.rmtree(output_dir, ignore_errors=True)

        jobs[job_id]["status"] = "done"
        jobs[job_id]["zip_path"] = str(zip_path)
        add_log(job_id, f"✅ Selesai! {len(files)} video siap didownload.")

    except Exception as e:
        jobs[job_id]["status"] = "error"
        add_log(job_id, f"❌ Error: {str(e)}")
        shutil.rmtree(output_dir, ignore_errors=True)


@app.get("/api/progress/{job_id}")
async def stream_progress(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job tidak ditemukan")

    async def generator():
        last_count = 0
        while True:
            job = jobs.get(job_id, {})
            logs = job.get("logs", [])
            new_logs = logs[last_count:]
            last_count = len(logs)

            data = {
                "status": job.get("status"),
                "progress": job.get("progress", 0),
                "total": job.get("total", 0),
                "new_logs": new_logs,
            }
            yield f"data: {json.dumps(data)}\n\n"

            if job.get("status") in ("done", "error"):
                break

            await asyncio.sleep(1)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.get("/api/zip/{job_id}")
async def download_zip(job_id: str):
    job = jobs.get(job_id)
    if not job or job.get("status") != "done":
        raise HTTPException(status_code=404, detail="File belum siap")

    zip_path = job["zip_path"]
    username = job["username"]

    async def cleanup():
        await asyncio.sleep(60)
        if os.path.exists(zip_path):
            os.remove(zip_path)
        jobs.pop(job_id, None)

    asyncio.create_task(cleanup())

    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"@{username}_tiktok.zip",
    )
