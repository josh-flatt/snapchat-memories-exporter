import subprocess
from tqdm.asyncio import tqdm
from pathlib import Path
import pandas as pd


file_directory = Path("./downloads")
files_to_check = [
    p
    for p in file_directory.iterdir()
    if p.is_file() and p.suffix.lower() in [".jpg", ".mp4"]
]

progress_bar = tqdm(
    total=len(files_to_check),
    desc="Gathering",
    unit="file",
    disable=False,
)

file_df = pd.DataFrame(data=files_to_check, columns=["file"])


def get_metadata(row: pd.Series) -> pd.Series:

    file_path = row["file"]
    command = [
        "exiftool",
        "-T",
        "-createdate",
        "-gpslatitude",
        "-gpslongitude",
        str(file_path),
    ]
    result = subprocess.run(
        command, capture_output=True, text=True, check=False, timeout=30
    )
    metadata = result.stdout.strip().split("\n")[0].split("\t")

    if len(metadata) < 3:
        metadata = metadata + ["-"] * (3 - len(metadata))
    row["createdate"] = metadata[0]
    row["gpslatitude"] = metadata[1]
    row["gpslongitude"] = metadata[2]
    progress_bar.update(1)
    return row


df = file_df.apply(get_metadata, axis=1)


df.to_csv("./temp/filemetadata.csv", index=False, sep="#")
