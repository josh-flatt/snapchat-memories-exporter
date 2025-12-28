import asyncio
import aiohttp
import json
import pytz
import time
import httpx
import os
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

CONCURRENCY = 50
RETRIES = 3
OUTPUT_DIR = Path("./downloads")
CHECKPOINT = Path("./temp/checkpoint.txt")


def load_checkpoint():
    if CHECKPOINT.exists():
        return set(CHECKPOINT.read_text().splitlines())
    return set()


def save_to_checkpoint(path: Path):
    with CHECKPOINT.open("a") as f:
        f.write(str(path) + "\n")


async def utc_filename(timestamp: str, url: str) -> Path:
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S UTC")
    dt = dt.replace(tzinfo=pytz.utc)
    ext = "mp4" if ".mp4" in url.lower() else "jpg"
    out_path = os.path.join("./downloads", f"{dt.strftime('%Y-%m-%d_%H-%M-%S')}.{ext}")
    return Path(out_path)


async def get_cdn_url(download_link: str) -> str:
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            download_link,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        return response.text.strip()


async def download_one(session, sem, url, timestamp, failures, stats):
    async with sem:
        for attempt in range(1, RETRIES + 1):
            try:
                cdn_url = await get_cdn_url(url)
                url = cdn_url
                path = await utc_filename(timestamp, url)
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    data = await resp.read()

                if path.exists():
                    return

                if str(path) in stats["done"]:
                    return

                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(data)
                stats["mb"] += len(data) / (1024 * 1024)
                save_to_checkpoint(path)
                stats["done"].add(str(path))

                return
            except Exception as e:
                if attempt == RETRIES:
                    failures.append((url, str(e)))
                await asyncio.sleep(0.3 * attempt)


async def main():
    with open("./resources/json/memories_history.json", "r") as f:
        memories = json.load(f)["Saved Media"]

    sem = asyncio.Semaphore(CONCURRENCY)
    async with sem:
        getting_urls = tqdm(total=len(memories), desc="Retrieving", unit="file")

        tasks_to_download = []

        for item in memories:
            url = item.get("Download Link")
            ts = item.get("Date")
            if not url or not ts:
                continue
            tasks_to_download.append((url, ts))
            getting_urls.update(1)
        getting_urls.close()

    stats = {"mb": 0.0, "done": load_checkpoint()}

    failures = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        start_time = time.time()

        to_process = [
            # if not path.exists() and str(path) not in stats["done"]
            (url, timestamp)
            for (url, timestamp) in tasks_to_download
        ]

        progress = tqdm(total=len(to_process), desc="Downloading", unit="file")

        async def wrapped(url, timestamp):
            await download_one(session, sem, url, timestamp, failures, stats)
            progress.update(1)

        await asyncio.gather(
            *(wrapped(url, timestamp) for url, timestamp in to_process)
        )
        progress.close()

        elapsed = time.time() - start_time

    mb_total = stats["mb"]
    speed = mb_total / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print(f"Downloaded: {len(to_process) - len(failures)} files")
    print(f"Failed:     {len(failures)} files")
    print(f"Data:       {mb_total:.2f} MB")
    print(f"Speed:      {speed:.2f} MB/s")
    print("=" * 60)

    if failures:
        print("\nFailed downloads:")
        for url, err in failures:
            print(f" - {url}   ({err})")
        print()


if __name__ == "__main__":
    asyncio.run(main())
