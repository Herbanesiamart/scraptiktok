import asyncio
import os
import uuid
import zipfile
import json
import shutil
from pathlib import Path
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import FileResponse, StreamingResponse, HTMLResponse

app = FastAPI()

APP_PASSWORD = os.environ.get("APP_PASSWORD", "adsy2024")

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
    cookies: UploadFile = File(...),
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

    job_id = str(uuid.uuid4())
    total = end - start + 1
    order = order if order in ("newest", "oldest") else "newest"

    # Simpan cookies.txt sementara
    cookies_path = f"/tmp/cookies_{job_id}.txt"
    content = await cookies.read()
    with open(cookies_path, "wb") as f:
        f.write(content)

    jobs[job_id] = {
        "status": "queued",
        "progress": 0,
        "total": total,
        "logs": [],
        "zip_path": None,
        "username": username,
    }

    background_tasks.add_task(run_download, job_id, username, start, end, order, cookies_path)

    return {"job_id": job_id}


def add_log(job_id: str, msg: str):
    if job_id in jobs:
        jobs[job_id]["logs"].append(msg)


async def run_download(job_id: str, username: str, start: int, end: int, order: str = "newest", cookies_path: str = None):
    output_dir = Path(f"/tmp/tktk_{job_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        jobs[job_id]["status"] = "downloading"
        add_log(job_id, f"🔍 Mencari video dari @{username}...")

        url = f"https://www.tiktok.com/@{username}"

        cmd = [
            "yt-dlp",
            "-I", f"{start}:{end}",
            "--merge-output-format", "mp4",
            "--recode-video", "mp4",
            "--postprocessor-args", "ffmpeg:-vcodec libx264 -acodec aac -movflags +faststart",
            "-o", str(output_dir / "%(autonumber)s_%(id)s.%(ext)s"),
            "--newline",
            "--no-warnings",
            url,
        ]

        if cookies_path and os.path.exists(cookies_path):
            cmd += ["--cookies", cookies_path]

        if order == "oldest":
            cmd.insert(1, "--playlist-reverse")

        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        downloaded = 0
        total = end - start + 1

        async for raw in process.stdout:
            line = raw.decode("utf-8", errors="ignore").strip()
            if not line:
                continue

            # Log semua baris supaya bisa debug
            add_log(job_id, line)

            if "Destination:" in line:
                downloaded += 1
                jobs[job_id]["progress"] = downloaded

            elif "has already been downloaded" in line:
                downloaded += 1
                jobs[job_id]["progress"] = downloaded

        await process.wait()

        files = list(output_dir.iterdir())
        if not files:
            jobs[job_id]["status"] = "error"
            add_log(job_id, "❌ Tidak ada video yang berhasil didownload. Cek username atau coba lagi.")
            return

        add_log(job_id, f"📦 Membuat ZIP dari {len(files)} video...")

        zip_path = Path(f"/tmp/tktk_{job_id}.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(files):
                zf.write(f, f.name)

        shutil.rmtree(output_dir, ignore_errors=True)
        if cookies_path and os.path.exists(cookies_path):
            os.remove(cookies_path)

        jobs[job_id]["status"] = "done"
        jobs[job_id]["zip_path"] = str(zip_path)
        add_log(job_id, f"✅ Selesai! {len(files)} video siap didownload.")

    except Exception as e:
        jobs[job_id]["status"] = "error"
        add_log(job_id, f"❌ Error tidak terduga: {str(e)}")
        shutil.rmtree(output_dir, ignore_errors=True)
        if cookies_path and os.path.exists(cookies_path):
            os.remove(cookies_path)


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

    # Cleanup after 60s
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
