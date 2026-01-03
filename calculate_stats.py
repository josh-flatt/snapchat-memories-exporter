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
        "-IFD0:DateTime",
        "-ExifIFD:DateTimeOriginal",
        "-ExifIFD:DateTimeDigitized",
        "-OffsetTimeOriginal",
        "-GPSLatitudeRef",
        "-GPSLongitudeRef",
        "-QuickTime:CreationDate",
        "-QuickTime:TrackCreateDate",
        "-QuickTime:MediaCreateDate",
        "-QuickTime:TimeZone",
        "-QuickTime:GPSCoordinates",
        "-xmp:gpslatitude",
        "-xmp:gpslongitude",
        "-QuickTime:LocationLatitude",
        "-QuickTime:LocationLongitude",
        str(file_path),
    ]
    result = subprocess.run(
        command, capture_output=True, text=True, check=False, timeout=30
    )
    metadata = result.stdout.strip().split("\n")[0].split("\t")

    row["createdate"] = metadata[0]
    row["gpslatitude"] = metadata[1]
    row["gpslongitude"] = metadata[2]
    # row["IFD0:DateTime"] = metadata[3]  # empty
    row["ExifIFD:DateTimeOriginal"] = metadata[4]
    # row["ExifIFD:DateTimeDigitized"] = metadata[5]  # empty
    row["OffsetTimeOriginal"] = metadata[6]
    row["GPSLatitudeRef"] = metadata[7]
    row["GPSLongitudeRef"] = metadata[8]
    row["QuickTime:CreationDate"] = metadata[9]
    row["QuickTime:TrackCreateDate"] = metadata[10]
    row["QuickTime:MediaCreateDate"] = metadata[11]
    # row["QuickTime:TimeZone"] = metadata[12]  # empty
    row["QuickTime:GPSCoordinates"] = metadata[13]
    row["xmp:gpslatitude"] = metadata[14]
    row["xmp:gpslongitude"] = metadata[15]
    # row["QuickTime:LocationLatitude"] = metadata[16]  # empty
    # row["QuickTime:LocationLongitude"] = metadata[17]  # empty
    row["long"] = get_longitude(row)
    row["lat"] = get_latitude(row)
    progress_bar.update(1)
    return row


def get_longitude(
    row: pd.Series,
) -> float | None:  # Changes DMS string to decimal degrees
    gps_str = row["gpslongitude"]
    if gps_str == "-":
        return None
    try:
        parts = (
            gps_str.replace(" deg ", "#")
            .replace('"', "#")
            .replace("'", "#")
            .replace(" ", "")
            .split("#")
        )
        degrees = float(parts[0])
        minutes = float(parts[1]) / 60.00
        seconds = float(parts[2]) / 3600.00
        longitude = degrees + minutes + seconds
        if gps_str[-1] in ["W", "S"]:
            longitude = -longitude
        return round(longitude, 6)
    except Exception:
        return None


def get_latitude(
    row: pd.Series,
) -> float | None:  # Changes DMS string to decimal degrees
    gps_str = row["gpslatitude"]
    if gps_str == "-":
        return None
    try:
        parts = (
            gps_str.replace(" deg ", "#")
            .replace('"', "#")
            .replace("'", "#")
            .replace(" ", "")
            .split("#")
        )
        degrees = float(parts[0])
        minutes = float(parts[1]) / 60.00
        seconds = float(parts[2]) / 3600.00
        latitude = degrees + minutes + seconds
        if gps_str[-1] in ["W", "S"]:
            latitude = -latitude
        return round(latitude, 6)
    except Exception:
        return None


df = file_df.apply(get_metadata, axis=1)
progress_bar.close()

print("Saving metadata to filemetadata.json")
df.to_json("./resources/temp/filemetadata.json", orient="index", default_handler=str)
