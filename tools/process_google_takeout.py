#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZipFile


MEDIA_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".heic",
    ".heif",
    ".webp",
    ".gif",
    ".mp4",
    ".mov",
    ".m4v",
    ".avi",
    ".mkv",
    ".3gp",
    ".mts",
    ".m2ts",
}

SIDECAR_SUFFIX = ".supplemental-metadata.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Extract Google Takeout media and merge sidecar metadata.")
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def is_trash_path(parts):
    return "Otpad" in parts or "Trash" in parts


def relative_takeout_path(name):
    parts = [part for part in name.replace("\\", "/").split("/") if part not in ("", ".", "..")]
    if is_trash_path(parts):
        return None

    prefixes = [
        ("Takeout", "Google fotografije"),
        ("Takeout", "Google Photos"),
    ]
    for prefix in prefixes:
        if tuple(parts[: len(prefix)]) == prefix:
            parts = parts[len(prefix) :]
            break

    if not parts:
        return None
    return Path(*parts)


def sidecar_key(rel_path):
    rel_text = rel_path.as_posix()
    if rel_text.endswith(SIDECAR_SUFFIX):
        return rel_text[: -len(SIDECAR_SUFFIX)]
    return None


def safe_target(output_dir, rel_path):
    target = (output_dir / rel_path).resolve()
    output_root = output_dir.resolve()
    if output_root not in target.parents and target != output_root:
        raise RuntimeError(f"Unsafe target path: {rel_path}")

    if not target.exists():
        return target, False

    stem = target.stem
    suffix = target.suffix
    counter = 1
    while True:
        candidate = target.with_name(f"{stem} ({counter}){suffix}")
        if not candidate.exists():
            return candidate, True
        counter += 1


def load_sidecars(zip_paths):
    sidecars = {}
    for zip_path in zip_paths:
        with ZipFile(zip_path) as archive:
            for info in archive.infolist():
                if info.is_dir() or not info.filename.endswith(SIDECAR_SUFFIX):
                    continue
                rel_path = relative_takeout_path(info.filename)
                if rel_path is None:
                    continue
                key = sidecar_key(rel_path)
                if not key:
                    continue
                try:
                    with archive.open(info) as fh:
                        sidecars[key] = json.load(fh)
                except json.JSONDecodeError:
                    pass
    return sidecars


def exif_datetime_from_timestamp(value):
    if value in (None, "", "0", 0):
        return None
    dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
    return dt.strftime("%Y:%m:%d %H:%M:%S")


def gps_args(meta):
    geo = meta.get("geoDataExif") or meta.get("geoData") or {}
    lat = geo.get("latitude")
    lon = geo.get("longitude")
    alt = geo.get("altitude")

    if lat in (None, 0, 0.0) and lon in (None, 0, 0.0):
        return []

    args = [
        f"-GPSLatitude={abs(float(lat))}",
        f"-GPSLatitudeRef={'S' if float(lat) < 0 else 'N'}",
        f"-GPSLongitude={abs(float(lon))}",
        f"-GPSLongitudeRef={'W' if float(lon) < 0 else 'E'}",
    ]
    if alt not in (None, 0, 0.0):
        args.append(f"-GPSAltitude={float(alt)}")
    return args


def metadata_args(meta):
    args = []
    timestamp = (meta.get("photoTakenTime") or {}).get("timestamp")
    exif_date = exif_datetime_from_timestamp(timestamp)
    if exif_date:
        args.extend(
            [
                f"-DateTimeOriginal={exif_date}",
                f"-CreateDate={exif_date}",
                f"-ModifyDate={exif_date}",
                f"-MediaCreateDate={exif_date}",
                f"-TrackCreateDate={exif_date}",
            ]
        )

    description = (meta.get("description") or "").strip()
    if description:
        args.extend([f"-Description={description}", f"-ImageDescription={description}"])

    args.extend(gps_args(meta))
    return args


def apply_metadata(path, meta):
    args = metadata_args(meta)
    if not args:
        return "no_metadata_fields"

    command = ["exiftool", "-m", "-overwrite_original", "-P", *args, str(path)]
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        return f"exiftool_error: {result.stderr.strip() or result.stdout.strip()}"
    return "metadata_applied"


def main():
    args = parse_args()
    zip_paths = sorted(args.input_dir.glob("*.zip"))
    if not zip_paths:
        raise SystemExit("No ZIP files found.")
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise SystemExit(f"Output directory already exists and is not empty: {args.output_dir}")

    sidecars = load_sidecars(zip_paths)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "zips": [str(path) for path in zip_paths],
        "sidecars_loaded": len(sidecars),
        "media_extracted": 0,
        "trash_skipped": 0,
        "json_skipped": 0,
        "non_media_skipped": 0,
        "metadata_applied": 0,
        "metadata_missing": 0,
        "metadata_errors": [],
        "renamed_collisions": 0,
        "files": [],
    }

    for zip_path in zip_paths:
        with ZipFile(zip_path) as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                rel_path = relative_takeout_path(info.filename)
                if rel_path is None:
                    report["trash_skipped"] += 1
                    continue
                if info.filename.endswith(".json"):
                    report["json_skipped"] += 1
                    continue
                if rel_path.suffix.lower() not in MEDIA_EXTENSIONS:
                    report["non_media_skipped"] += 1
                    continue

                target, renamed = safe_target(args.output_dir, rel_path)
                if renamed:
                    report["renamed_collisions"] += 1
                if not args.dry_run:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(info) as src, open(target, "wb") as dst:
                        shutil.copyfileobj(src, dst, 1024 * 1024)

                key = rel_path.as_posix()
                meta = sidecars.get(key)
                status = "metadata_missing"
                if meta and not args.dry_run:
                    status = apply_metadata(target, meta)
                elif meta:
                    status = "metadata_available"

                if status == "metadata_applied":
                    report["metadata_applied"] += 1
                elif status.startswith("exiftool_error"):
                    report["metadata_errors"].append({"file": str(target), "error": status})
                else:
                    report["metadata_missing"] += 1

                report["media_extracted"] += 1
                report["files"].append({"source": info.filename, "target": str(target), "metadata": status})

    report_path = args.output_dir / "_processing-report.json"
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)

    print(json.dumps({k: v for k, v in report.items() if k != "files"}, ensure_ascii=False, indent=2))
    print(f"report={report_path}")


if __name__ == "__main__":
    main()
