import pandas as pd
import re
import pytz
import os
from timezonefinder import TimezoneFinder
from datetime import datetime
import subprocess
from tqdm.asyncio import tqdm
import string


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


df: pd.DataFrame = pd.read_json("./resources/temp/file_info.json", orient="records")

###############################
# def get_join_col(row: pd.Series) -> pd.Series:
#     row["file_dt"] = f"{row['Date'].strftime('%Y-%m-%d_%H-%M-%S')}"
#     return row

timestamp_counter = {}


def get_join_col(row: pd.Series) -> pd.Series:
    ts_str = row["Date"].strftime("%Y-%m-%d_%H-%M-%S")

    index = timestamp_counter.get(ts_str, 0)
    timestamp_counter[ts_str] = index + 1

    suffix = number_to_letters(index)
    row["file_dt"] = f"{ts_str}-{suffix}"
    return row


##################################################


def extract_lat_long(row: pd.Series) -> pd.Series:
    coordinates = row["Location"].split(": ")[1]
    lat_str, long_str = coordinates.split(", ")
    row["location_latitude"] = round(float(lat_str), 5)
    row["location_longitude"] = round(float(long_str), 5)
    return row


df = df.apply(get_join_col, axis=1)
df = df.apply(extract_lat_long, axis=1)


metadata = pd.read_csv("./resources/temp/filemetadata.csv", sep=",")
metadata["file"] = f"./" + metadata["file"]


def convert_lat_long_to_decimal_degrees(row: pd.Series) -> pd.Series:
    def dms_to_decimal(dms_string):
        match = re.search(
            r"(\d+)\s*deg\s*(\d+)'\s*([\d.]+)\"\s*([NSEW])", dms_string, re.IGNORECASE
        )
        if not match:
            raise ValueError(f"Coordinate format not recognized: {dms_string}")

        degrees = float(match.group(1))
        minutes = float(match.group(2))
        seconds = float(match.group(3))
        direction = match.group(4).upper()
        decimal_degrees = degrees + (minutes / 60) + (seconds / 3600)
        if direction in ["S", "W"]:
            decimal_degrees *= -1
        decimal_degrees = round(decimal_degrees, 5)
        return decimal_degrees

    def convert(value: str) -> float:
        if pd.isna(value):
            return 0.0
        if value == "-":
            return 0.0
        try:
            return dms_to_decimal(value)
        except ValueError:
            return 0.0

    row["Latitude"] = convert(row["gpslatitude"])
    row["Longitude"] = convert(row["gpslongitude"])
    return row


#############################################
# def get_file_name(row: pd.Series) -> pd.Series:
#     path_parts = row["file"].split("/")
#     file = path_parts[-1]
#     row["filename"] = file
#     row["extension"] = file.split(".")[-1].lower()
#     row["file_dt"] = file.split(".")[0]
#     return row
def get_file_name(row: pd.Series) -> pd.Series:
    file = os.path.basename(row["file"])
    name, ext = os.path.splitext(file)

    row["filename"] = file
    row["extension"] = ext[1:].lower()

    # name is YYYY-MM-DD_HH-MM-SS-A
    row["file_dt"] = name

    return row


#############################################

meta = metadata.apply(convert_lat_long_to_decimal_degrees, axis=1)
meta = meta.apply(get_file_name, axis=1)


def get_datetime_for_df(row: pd.Series) -> pd.Series:

    try:
        row["createdate_mod"] = None
        if row["createdate"] != "-":
            createdate = datetime.strptime(row["createdate"], "%Y:%m:%d %H:%M:%S")
            createdate_wtz = createdate.astimezone(pytz.timezone("America/Chicago"))
            row["createdate_mod"] = createdate_wtz

        #######################################
        # date = datetime.strptime(row["file_dt"], "%Y-%m-%d_%H-%M-%S")
        base_dt = row["file_dt"].rsplit("-", 1)[0]
        date = datetime.strptime(base_dt, "%Y-%m-%d_%H-%M-%S")

        #######################################
        dt_w_tz = date.astimezone(pytz.UTC)
        row["datetime"] = dt_w_tz
        row["dt_join"] = f"{dt_w_tz}"
    except ValueError:
        row["createdate_mod"] = None
        row["datetime"] = None
        row["dt_join"] = ""
    return row


def get_media_type(row: pd.Series) -> pd.Series:
    row["file_type"] = "Unknown"
    if row["extension"] == "jpg":
        row["file_type"] = "Image"
    if row["extension"] == "mp4":
        row["file_type"] = "Video"
    return row


data = meta.apply(get_datetime_for_df, axis=1)
data = data.apply(get_media_type, axis=1)
joined = pd.merge(
    df,
    data,
    how="left",
    left_on="file_dt",
    right_on="file_dt",
    suffixes=("_df", "_meta"),
)


col_mapper = {
    "Date": "correct_date_utc",
    "Media Type": "correct_media_type",
    "location_latitude": "correct_latitude",
    "location_longitude": "correct_longitude",
    "file": "path",
    "filename": "filename",
    "createdate": "actual_date_cst",
    "Latitude": "actual_latitude",
    "Longitude": "actual_longitude",
    "file_type": "actual_media_type",
}
df0 = joined[col_mapper.keys()].rename(columns=col_mapper)


def check_for_errors(row: pd.Series) -> pd.Series:
    errors = 0
    if row["correct_media_type"] != row["actual_media_type"]:
        errors += 1
    if row["actual_latitude"] == 0.00 and row["correct_media_type"] == "Image":
        if row["correct_latitude"] != 0.00:
            errors += 1
    if abs(row["actual_longitude"] - row["correct_longitude"]) > 2.00:
        errors += 1
    if row["actual_date_cst"] == "-":
        errors += 1
    if pd.isna(row["path"]):
        errors += 1
    row["errors"] = errors
    return row


df1 = df0.apply(check_for_errors, axis=1)

errors = df1[df1["errors"] > 0]
print(f"Errors: {errors.shape[0]}")

img_errors = errors[errors["correct_media_type"] == "Image"]
print(f"Image errors: {img_errors.shape[0]}")

video_errors = errors[errors["correct_media_type"] == "Video"]
print(f"Video errors: {video_errors.shape[0]}")


def fix_filetype(row: pd.Series) -> pd.Series:
    try:
        if pd.isna(row["path"]) or row["path"] == "-":
            return row

        mp4_file_path = f"./downloads/{row['filename'][:-4]}.mp4"
        jpg_file_path = f"./downloads/{row['filename'][:-4]}.jpg"

        if row["correct_media_type"] == row["actual_media_type"]:
            return row

        if row["path"] == "-":
            return row

        if row["correct_media_type"] == "Image":
            os.rename(src=mp4_file_path, dst=jpg_file_path)
            if os.path.exists(jpg_file_path):
                row["filename"] = f"{row['filename'][:-4]}.jpg"
                row["actual_media_type"] = "Image"
        elif row["correct_media_type"] == "Video":
            os.rename(src=jpg_file_path, dst=mp4_file_path)
            if os.path.exists(mp4_file_path):
                row["filename"] = f"{row['filename'][:-4]}.mp4"
                row["actual_media_type"] = "Video"
    except FileNotFoundError:
        print(f"File not found: {row['filename']}")
    return row


df1.apply(fix_filetype, axis=1)


tf = TimezoneFinder()


def get_localized_dt_and_offset(
    utc_dt: datetime, latitude: float, longitude: float
) -> tuple[str, str]:
    tz_name = tf.timezone_at(lng=longitude, lat=latitude)

    utc_timezone = pytz.utc
    central_timezone = pytz.timezone("America/Chicago")
    mountain_timezone = pytz.timezone("America/Denver")

    if not tz_name or latitude == 0 or longitude == 0:
        print(
            f"⚠️ Warning: No timezone found for {utc_dt.strftime("%Y-%m-%d %H:%M:%S")} UTC. Defaulting to MDT/MST."
        )
        local_tz = mountain_timezone
    else:
        local_tz = pytz.timezone(tz_name)

    dt_local = utc_dt.astimezone(local_tz)

    dt_str = dt_local.strftime("%Y:%m:%d %H:%M:%S")

    # Tags require offset in ±HH:MM format
    offset_str: str = dt_local.strftime("%z")
    if len(offset_str) == 5:
        offset_str = offset_str[:3] + ":" + offset_str[3:]

    return dt_str, offset_str


progress_bar = tqdm(
    total=df1.shape[0],
    desc="Updating EXIF",
    unit="file",
    disable=False,
)


def update_exif_with_exiftool(row: pd.Series) -> None:
    progress_bar.update(1)
    image_path = row["path"]
    dt_utc = row["correct_date_utc"]  # timezone-aware, UTC datetime
    latitude = row["correct_latitude"]
    longitude = row["correct_longitude"]
    file_type = row["correct_media_type"]
    abs_latitude = abs(latitude)
    abs_longitude = abs(longitude)

    dt_utc_str = dt_utc.strftime("%Y:%m:%d %H:%M:%S")

    if image_path is None or pd.isna(image_path) or image_path == "":
        print(f"❌ File not found, skipping: {dt_utc_str}")
        return

    lat_ref = "N" if latitude >= 0 else "S"
    lon_ref = "E" if longitude >= 0 else "W"

    local_dt, dynamic_tz = get_localized_dt_and_offset(
        utc_dt=dt_utc,
        latitude=row["correct_latitude"],
        longitude=row["correct_longitude"],
    )

    if not os.path.exists(image_path):
        print(f"❌ File not found, skipping: {image_path}")
        return

    jpg_command = [
        "exiftool",
        # Date/Time tags
        f"-IFD0:DateTime={local_dt}",
        f"-ExifIFD:DateTimeOriginal={local_dt}",
        f"-ExifIFD:DateTimeDigitized={local_dt}",
        f"-OffsetTimeOriginal={dynamic_tz}",
        # Location tags
        f"-GPSLatitude={latitude}",
        f"-GPSLongitude={longitude}",
        f"-GPSLatitudeRef='{lat_ref}'",
        f"-GPSLongitudeRef='{lon_ref}'",
        "-overwrite_original",
        image_path,
    ]
    jpg_command_no_gps = [
        "exiftool",
        # Date/Time tags
        f"-IFD0:DateTime={local_dt}",
        f"-ExifIFD:DateTimeOriginal={local_dt}",
        f"-ExifIFD:DateTimeDigitized={local_dt}",
        f"-OffsetTimeOriginal={dynamic_tz}",
        "-overwrite_original",
        image_path,
    ]
    mp4_command = [
        "exiftool",
        # Date/Time tags
        # EXIF Tags
        f"-IFD0:DateTime={local_dt}",
        f"-ExifIFD:DateTimeOriginal={local_dt}",
        f"-ExifIFD:DateTimeDigitized={local_dt}",
        f"-OffsetTimeOriginal={dynamic_tz}",
        # QuickTime tags (must be set to UTC time)
        f"-QuickTime:CreationDate={dt_utc_str}",
        f"-QuickTime:TrackCreateDate={dt_utc_str}",
        f"-QuickTime:MediaCreateDate={dt_utc_str}",
        f"-QuickTime:TimeZone={dynamic_tz}",
        # Location tags
        # Needed for location on Google Photos
        f"-QuickTime:GPSCoordinates={abs_latitude} {lat_ref}, {abs_longitude} {lon_ref}",
        # XMP Tags (Adobe/Google
        f"-xmp:gpslatitude={latitude}",
        f"-xmp:gpslongitude={longitude}",
        # QuickTime Tags (Apple/iOS)
        f"-QuickTime:LocationLatitude={latitude}",
        f"-QuickTime:LocationLongitude={longitude}",
        # # Generic GPS tags (Broader compatibility)
        f"-GPSLatitude={latitude}",
        f"-GPSLongitude={longitude}",
        f"-GPSLatitudeRef={lat_ref}",
        f"-GPSLongitudeRef={lon_ref}",
        "-overwrite_original",
        image_path,
    ]
    mp4_command_no_gps = [
        "exiftool",
        # EXIF Tags
        f"-IFD0:DateTime={local_dt}",
        f"-ExifIFD:DateTimeOriginal={local_dt}",
        f"-ExifIFD:DateTimeDigitized={local_dt}",
        f"-OffsetTimeOriginal={dynamic_tz}",
        # QuickTime tags (must be set to UTC time)
        f"-QuickTime:CreationDate={dt_utc_str}",
        f"-QuickTime:TrackCreateDate={dt_utc_str}",
        f"-QuickTime:MediaCreateDate={dt_utc_str}",
        f"-QuickTime:TimeZone={dynamic_tz}",
        "-overwrite_original",
        image_path,
    ]

    try:
        if file_type == "Video":
            if latitude == 0.0 and longitude == 0.0:
                result = subprocess.run(
                    mp4_command_no_gps, check=True, capture_output=True, text=True
                )
            else:
                result = subprocess.run(
                    mp4_command, check=True, capture_output=True, text=True
                )
        elif file_type == "Image":
            if latitude == 0.0 and longitude == 0.0:
                result = subprocess.run(
                    jpg_command_no_gps, check=True, capture_output=True, text=True
                )
            else:
                result = subprocess.run(
                    jpg_command, check=True, capture_output=True, text=True
                )
        else:
            print(f"⚠️ Warning: Unknown media type for {image_path}, skipping.")
            return

        if "1 image files updated" in result.stdout:
            pass
            # print(f"✅ Updated: {image_path}")
        else:
            print(f"⚠️ Warning updating {image_path}: {result.stderr.strip()}")

    except subprocess.CalledProcessError as e:
        print(f"🛑 Subprocess Error on {image_path}: {e.stderr.strip()}")
    except FileNotFoundError:
        print(
            "🛑 ERROR: ExifTool command not found. Is ExifTool installed and in your PATH?"
        )


df1.apply(update_exif_with_exiftool, axis=1)  # type: ignore

progress_bar.close()
