import pandas as pd
import re
import pytz
import os
from timezonefinder import TimezoneFinder
from datetime import datetime, timedelta, timezone
import subprocess
from tqdm.asyncio import tqdm
import json
import string
from pathlib import Path


OUTPUT_DIR = Path("./downloads")


def number_to_letters(n: int) -> str:
    """0 -> A, 25 -> Z, 26 -> AA, etc."""
    letters = string.ascii_uppercase
    result = ""
    while True:
        n, rem = divmod(n, 26)
        result = letters[rem] + result
        if n == 0:
            break
        n -= 1
    return result


def get_file_path(timestamp: str, index: int, url: str) -> Path:
    dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S UTC")
    dt = dt.replace(tzinfo=pytz.utc)
    ext = "mp4" if ".mp4" in url.lower() else "jpg"
    suffix = f"-{number_to_letters(index)}"
    filename = f"{dt.strftime('%Y-%m-%d_%H-%M-%S')}{suffix}.{ext}"
    return OUTPUT_DIR / filename


# Load original JSON
with open("./resources/json/memories_history.json", "r") as f:
    memories = json.load(f)["Saved Media"]

# Prepare index per timestamp (deterministic order like downloader)
timestamp_index_map = {}

records = []
for item in memories:
    url = item.get("Download Link")
    ts = item.get("Date")
    if not url or not ts:
        continue

    index = timestamp_index_map.get(ts, 0)
    timestamp_index_map[ts] = index + 1

    path = get_file_path(ts, index, url)

    # Add everything to a record dictionary
    record = item.copy()
    record["file_path"] = path
    records.append(record)

# Create DataFrame
df = pd.DataFrame(records)


def _get_filename(row: pd.Series) -> pd.Series:
    path_parts = f"{row["file_path"]}".split("/")
    file = path_parts[-1]
    row["filename"] = file[0:-4]
    return row


def get_utc_datetime(row: pd.Series) -> pd.Series:
    row["Date"] = pd.to_datetime(row["Date"], format="%Y-%m-%d %H:%M:%S UTC", utc=True)
    return row


correct = df
correct = correct.apply(_get_filename, axis=1)
correct = correct.apply(get_utc_datetime, axis=1)


current: pd.DataFrame = pd.read_json(
    "./resources/temp/filemetadata.json", orient="index"
)
current["file"] = f"./" + current["file"]


def get_filetype(row: pd.Series) -> pd.Series:
    row["filetype"] = "Unknown"
    extension = row["file"].split(".")[-1]
    if extension == "jpg":
        row["filetype"] = "Image"
    if extension == "mp4":
        row["filetype"] = "Video"
    return row


def get_dd_from_dms(
    coord_str: str,
) -> float | None:  # Changes DMS string to decimal degrees
    gps_str = coord_str
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
        dd_coordinate = degrees + minutes + seconds
        if gps_str[-1] in ["W", "S"]:
            dd_coordinate = -dd_coordinate
        return round(dd_coordinate, 6)
    except Exception:
        return None


def get_jpg_coords(row: pd.Series) -> pd.Series:
    row["jpg_latitude"] = None
    row["jpg_longitude"] = None
    if row["gpslatitude"] != "-" and row["gpslongitude"] != "-":
        row["jpg_latitude"] = get_dd_from_dms(row["gpslatitude"])
        row["jpg_longitude"] = get_dd_from_dms(row["gpslongitude"])
        return row
    return row


def get_mp4_coords(row: pd.Series) -> pd.Series:
    row["mp4_latitude"] = None
    row["mp4_longitude"] = None
    row["mp4_coord_err_flag"] = True
    if row["QuickTime:GPSCoordinates"] != "-":
        coords = row["QuickTime:GPSCoordinates"].split(", ")
        row["mp4_latitude"] = get_dd_from_dms(coords[0])
        row["mp4_longitude"] = get_dd_from_dms(coords[1])
        if coords[0] == row["gpslatitude"] and coords[1] == row["gpslongitude"]:
            if (
                coords[0] == row["xmp:gpslatitude"]
                and coords[1] == row["xmp:gpslongitude"]
            ):
                row["mp4_coord_err_flag"] = False
        return row
    return row


def get_jpg_times(row: pd.Series) -> pd.Series:
    row["jpg_utc_time"] = None
    row["jpg_local_tz"] = None
    if row["ExifIFD:DateTimeOriginal"] != "-":
        local_time_str = f"{row["ExifIFD:DateTimeOriginal"]}{row['OffsetTimeOriginal']}"
        local_time = pd.to_datetime(local_time_str, format="%Y:%m:%d %H:%M:%S%z")
        utc_time = local_time.astimezone(pytz.UTC)
        row["jpg_utc_time"] = utc_time
        row["jpg_local_tz"] = f"{row["OffsetTimeOriginal"]}"
    return row


def get_mp4_times(row: pd.Series) -> pd.Series:
    row["mp4_utc_time"] = None
    row["mp4_local_tz"] = None
    if row["QuickTime:CreationDate"] != "-":
        utc_time_str = f"{row["QuickTime:CreationDate"][0:-6]}+00:00"  # Creationdate is in UTC but appending local offset (shouldn't be)
        local_offset = row["QuickTime:CreationDate"][-6:]
        utc_time = pd.to_datetime(utc_time_str, format="%Y:%m:%d %H:%M:%S%z", utc=True)
        row["mp4_utc_time"] = utc_time
        row["mp4_local_tz"] = local_offset
    return row


current = current.apply(get_filetype, axis=1)
current = current.apply(get_jpg_coords, axis=1)
current = current.apply(get_mp4_coords, axis=1)
current = current.apply(get_jpg_times, axis=1)
current = current.apply(get_mp4_times, axis=1)


current_keep = [
    "file",
    "filetype",
    # "createdate",
    #  -> appears to be originating from file actually downloaded
    #  -> UTC, not local time
    # "ExifIFD:DateTimeOriginal",
    #  -> coming from the "Date" field in the memories_history.json file
    #  -> is in local time, not UTC
    # "OffsetTimeOriginal",
    #  -> Local timezone offset, used for jpg files
    # "QuickTime:CreationDate",
    #  -> coming from the "Date" field in the memories_history.json file
    #  -> is UTC time plus local timezone offset
    "jpg_latitude",
    "jpg_longitude",
    "mp4_latitude",
    "mp4_longitude",
    "jpg_utc_time",
    "jpg_local_tz",
    "mp4_utc_time",
    "mp4_local_tz",
]
# Google photos is using for date/time:
#   mp4 -> Appears to be QuickTime:CreationDate, not createdate
#   jpg -> Uses ExifIFD:DateTimeOriginal
# Google photos is using for location:
#   mp4 -> Appears to be QuickTime:GPSCoordinates, not gpslatitude/gpslongitude
#   jpg -> Appears to be gpslatitude/gpslongitude
df1 = current[current_keep]


def get_filename(row: pd.Series) -> pd.Series:
    path_parts = row["file"].split("/")
    file = path_parts[-1]
    row["filename"] = file[0:-4]
    return row


df1 = df1.apply(get_filename, axis=1)


def get_join_col(row: pd.Series) -> pd.Series:
    row["file_dt"] = f"{row['Date'].strftime('%Y-%m-%d_%H-%M-%S')}"
    return row


def extract_lat_long(row: pd.Series) -> pd.Series:
    coordinates = row["Location"].split(": ")[1]
    lat_str, long_str = coordinates.split(", ")
    row["location_latitude"] = round(float(lat_str), 5)
    row["location_longitude"] = round(float(long_str), 5)
    return row


keep_cols = {
    "filename": "file",
    "Media Type": "correct_filetype",
    "Date": "correct_datetime_utc",
    "location_latitude": "correct_latitude",
    "location_longitude": "correct_longitude",
}
correct = correct.apply(extract_lat_long, axis=1)
correct = correct[keep_cols.keys()].rename(columns=keep_cols)


tf = TimezoneFinder()


def get_localized_dt_and_offset(
    utc_dt: datetime, latitude: float, longitude: float
) -> tuple[datetime, str]:
    tz_name = tf.timezone_at(lng=longitude, lat=latitude)

    utc_dt = utc_dt.astimezone(tz=pytz.utc)

    utc_timezone = pytz.utc
    central_timezone = pytz.timezone("America/Chicago")
    mountain_timezone = pytz.timezone("America/Denver")

    if not tz_name or latitude == 0 or longitude == 0:
        # print(
        #     f"Warning: No timezone found for {utc_dt.strftime("%Y-%m-%d %H:%M:%S")} UTC. Defaulting to MDT/MST."
        # )
        local_tz = mountain_timezone
    else:
        local_tz = pytz.timezone(tz_name)

    dt_local = utc_dt.astimezone(local_tz)

    dt_str = dt_local.strftime("%Y:%m:%d %H:%M:%S")

    # Tags require offset in ±HH:MM format
    offset_str: str = dt_local.strftime("%z")
    if len(offset_str) == 5:
        offset_str = offset_str[:3] + ":" + offset_str[3:]

    return dt_local, offset_str


def get_localized_dt(row: pd.Series) -> pd.Series:
    local_dt, dynamic_tz = get_localized_dt_and_offset(
        utc_dt=row["correct_datetime_utc"],
        latitude=row["correct_latitude"],
        longitude=row["correct_longitude"],
    )
    row["correct_datetime_local"] = local_dt
    row["correct_tz"] = dynamic_tz
    return row


df = correct.apply(get_localized_dt, axis=1)


joined = pd.merge(
    df,
    df1,
    how="left",
    left_on="file",
    right_on="filename",
    suffixes=("_correct", "_current"),
)


def check_filetype(row: pd.Series) -> pd.Series:
    row["filetype_error"] = False
    if row["correct_filetype"] != row["filetype"]:
        row["filetype_error"] = True
    return row


def check_utc_datetime(row: pd.Series) -> pd.Series:
    row["utc_datetime_error"] = False
    row["utc_diff"] = None
    correct_utc_time = row["correct_datetime_utc"]
    current_utc_time = datetime.now()
    if row["filetype"] == "Image":
        current_utc_time = row["jpg_utc_time"]
    elif row["filetype"] == "Video":
        current_utc_time = row["mp4_utc_time"]

    current_utc_time = current_utc_time.replace(tzinfo=timezone.utc)
    correct_utc_time = correct_utc_time.replace(tzinfo=timezone.utc)
    utc_time_diff = correct_utc_time - current_utc_time
    row["utc_diff"] = utc_time_diff
    if abs(utc_time_diff.seconds) > 10:
        row["utc_datetime_error"] = True
    return row


def check_latitude(row: pd.Series) -> pd.Series:
    row["latitude_error"] = False
    correct_latitude = row["correct_latitude"]
    current_latitude = 0.00
    if correct_latitude == 0.00:
        row["latitude_error"] = True
        return row
    if row["correct_filetype"] == "Image":
        current_latitude = row["jpg_latitude"]
    elif row["correct_filetype"] == "Video":
        current_latitude = row["mp4_latitude"]
    latitude_diff = abs(correct_latitude - current_latitude)
    if latitude_diff > 0.0001:
        row["latitude_error"] = True
    return row


def check_longitude(row: pd.Series) -> pd.Series:
    row["longitude_error"] = False
    correct_longitude = row["correct_longitude"]
    current_longitude = 0.00
    if correct_longitude == 0.00:
        row["longitude_error"] = True
        return row
    if row["correct_filetype"] == "Image":
        current_longitude = row["jpg_longitude"]
    elif row["correct_filetype"] == "Video":
        current_longitude = row["mp4_longitude"]
    longitude_diff = abs(correct_longitude - current_longitude)
    if longitude_diff > 0.0001:
        row["longitude_error"] = True
    return row


def check_timezone(row: pd.Series) -> pd.Series:
    row["tz_error"] = False
    correct_tz = row["correct_tz"]
    current_tz = ""
    if row["correct_filetype"] == "Image":
        current_tz = row["jpg_local_tz"]
    elif row["correct_filetype"] == "Video":
        current_tz = row["mp4_local_tz"]
    if correct_tz != current_tz:
        row["tz_error"] = True
    return row


def get_need_fix(row: pd.Series) -> pd.Series:
    row["need_fix"] = False
    if (
        row["filetype_error"]
        or row["utc_datetime_error"]
        or row["latitude_error"]
        or row["longitude_error"]
        or row["tz_error"]
    ):
        row["need_fix"] = True
    return row


errors = joined.apply(check_filetype, axis=1)
errors = errors.apply(check_utc_datetime, axis=1)
errors = errors.apply(check_latitude, axis=1)
errors = errors.apply(check_longitude, axis=1)
errors = errors.apply(check_timezone, axis=1)
errors = errors.apply(get_need_fix, axis=1)


needs_fix = errors[errors["need_fix"]]


needs_fix.to_json(
    "./resources/temp/needs_fix.json", orient="index", default_handler=str, indent=4
)
