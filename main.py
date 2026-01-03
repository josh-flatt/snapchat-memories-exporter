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
import string

CONCURRENCY = 50
RETRIES = 3
OUTPUT_DIR = Path("./downloads")
CHECKPOINT = Path("./resources/temp/checkpoint.txt")


def load_checkpoint():
    if CHECKPOINT.exists():
        return set(CHECKPOINT.read_text().splitlines())
    return set()


def save_to_checkpoint(path: Path):
    with CHECKPOINT.open("a") as f:
        f.write(str(path) + "\n")


def number_to_letters(n: int) -> str:
    """
    Convert a 0-indexed number to letters (A-Z, AA-ZZ, etc.)
    0 -> A, 25 -> Z, 26 -> AA, 27 -> AB, ...
    """
    letters = string.ascii_uppercase
    result = ""
    while True:
        n, rem = divmod(n, 26)
        result = letters[rem] + result
        if n == 0:
            break
        n -= 1  # Adjust for 1-based carry
    return result


def get_letter_suffix(index: int) -> str:
    """
    Return letter suffix for a given index
    """
    return f"-{number_to_letters(index)}"


async def utc_filename(timestamp: str, url: str, index: int) -> Path:
    """
    Return a Path with format YYYY-MM-DD_HH-MM-SS-A.jpg/mp4
    Supports multiple files per second with AA, AB, ... if needed
    """
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S UTC")
    dt = dt.replace(tzinfo=pytz.utc)
    ext = "mp4" if ".mp4" in url.lower() else "jpg"
    suffix = get_letter_suffix(index)
    filename = f"{dt.strftime('%Y-%m-%d_%H-%M-%S')}{suffix}.{ext}"
    return OUTPUT_DIR / filename


async def get_cdn_url(download_link: str) -> str:
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            download_link,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        return response.text.strip()


async def download_one(session, sem, url, timestamp, index, failures, stats):
    async with sem:
        for attempt in range(1, RETRIES + 1):
            try:
                cdn_url = await get_cdn_url(url)
                url = cdn_url
                path = await utc_filename(timestamp, url, index)
                if path.exists() or str(path) in stats["done"]:
                    return

                async with session.get(url) as resp:
                    resp.raise_for_status()
                    data = await resp.read()

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

    # Prepare deterministic index per timestamp
    timestamp_index_map = {}
    tasks_to_download = []

    for item in memories:
        url = item.get("Download Link")
        ts = item.get("Date")
        if not url or not ts:
            continue

        # Determine deterministic index for this timestamp
        index = timestamp_index_map.get(ts, 0)
        timestamp_index_map[ts] = index + 1

        tasks_to_download.append((url, ts, index))

    stats = {"mb": 0.0, "done": load_checkpoint()}
    failures = []
    sem = asyncio.Semaphore(CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        start_time = time.time()
        progress = tqdm(total=len(tasks_to_download), desc="Downloading", unit="file")

        async def wrapped(url, timestamp, index):
            await download_one(session, sem, url, timestamp, index, failures, stats)
            progress.update(1)

        await asyncio.gather(
            *(
                wrapped(url, timestamp, index)
                for url, timestamp, index in tasks_to_download
            )
        )
        progress.close()

        elapsed = time.time() - start_time

    mb_total = stats["mb"]
    speed = mb_total / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 60)
    print(f"Downloaded: {len(tasks_to_download) - len(failures)} files")
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
