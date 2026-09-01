#!/usr/bin/env python3
"""
GBFS poller: takes a snapshot of a shared-mobility system and appends it to a
daily CSV file.

Run it on a schedule (every 5 minutes). Each run adds one row per station
(or per vehicle) to today's file. Over months, those files become a dataset
that nobody else has, because GBFS feeds are overwritten and never archived.

Usage:
    python gbfs_poll.py --url https://gbfs.bcycle.com/bcycle_lametro/gbfs.json \
                        --system metro_bike_share
"""

import argparse
import csv
import datetime as dt
import gzip
import json
import os
import shutil
import sys
import time
import urllib.error
import urllib.request

# Identify yourself. Feed operators can see this, and a real contact string is
# both polite and the reason you're less likely to get blocked.
USER_AGENT = "student-research-project/1.0 (bikeshare pricing study)"
TIMEOUT_SECONDS = 20
RETRIES = 3


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

def fetch_json(url):
    """GET a URL and parse it as JSON, retrying with backoff on failure.

    Networks fail. If this script crashes on a single timeout, your scheduler
    logs an error and you lose the observation. Retrying costs nothing.
    """
    last_error = None
    for attempt in range(RETRIES):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as error:
            last_error = error
            if attempt < RETRIES - 1:
                time.sleep(2 ** attempt)  # 1s, then 2s
    raise RuntimeError(f"failed to fetch {url}: {last_error}")


def discover_feeds(autodiscovery_url):
    """Read gbfs.json and return a mapping of feed name -> feed URL.

    gbfs.json is the index file: it tells you where every other file lives.
    Always start here rather than guessing URLs, because operators host their
    files at different paths.

    Annoyance: GBFS v1 and v2 nest the feed list under a language code
    (data -> "en" -> feeds) while v3.0 dropped that (data -> feeds). LA has
    systems on v1.1, v2.2 and v2.3, so handle both shapes.
    """
    document = fetch_json(autodiscovery_url)
    data = document.get("data", {})

    if "feeds" in data:
        feed_list = data["feeds"]                      # v3.0 shape
    else:
        first_language = next(iter(data.values()), {})  # v1 / v2 shape
        feed_list = first_language.get("feeds", [])

    return {entry["name"]: entry["url"] for entry in feed_list}


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------

def normalise_timestamp(value):
    """Return an ISO-8601 UTC string, whatever format the feed used.

    v1 and v2 report POSIX integers (1723478400). v3.0 switched to RFC3339
    strings ("2026-08-31T14:00:00Z"). Storing them in one consistent format now
    saves you from a miserable afternoon in six months.
    """
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, tz=dt.timezone.utc).isoformat()
    return str(value)


def first_present(dictionary, *keys):
    """Return the first key that exists. Field names moved between versions:
    v2 says num_bikes_available, v3 says num_vehicles_available."""
    for key in keys:
        if key in dictionary and dictionary[key] is not None:
            return dictionary[key]
    return ""


# ---------------------------------------------------------------------------
# Turning a feed into rows
# ---------------------------------------------------------------------------

STATION_COLUMNS = [
    "fetched_at",      # when YOUR script ran
    "last_updated",    # when the OPERATOR published the feed
    "last_reported",   # when THE STATION last phoned home
    "station_id",
    "bikes_available",
    "docks_available",
    "ebikes_available",
    "is_renting",
    "is_returning",
    "is_installed",
]

VEHICLE_COLUMNS = [
    "fetched_at",
    "last_updated",
    "vehicle_id",
    "lat",
    "lon",
    "is_reserved",
    "is_disabled",
    "vehicle_type_id",
    "current_range_meters",
]


def station_rows(feed, fetched_at):
    """Flatten station_status.json into one row per station.

    Three separate timestamps go into every row and they are genuinely
    different things. If a station's last_reported is two hours stale, its
    bike count is a guess, not an observation. Recording all three is what
    lets you tell those apart later. Most people only keep one and can never
    recover the distinction.
    """
    last_updated = normalise_timestamp(feed.get("last_updated"))
    rows = []
    for station in feed.get("data", {}).get("stations", []):
        rows.append({
            "fetched_at": fetched_at,
            "last_updated": last_updated,
            "last_reported": normalise_timestamp(station.get("last_reported")),
            "station_id": station.get("station_id", ""),
            "bikes_available": first_present(
                station, "num_bikes_available", "num_vehicles_available"),
            "docks_available": first_present(station, "num_docks_available"),
            "ebikes_available": first_present(station, "num_ebikes_available"),
            "is_renting": first_present(station, "is_renting"),
            "is_returning": first_present(station, "is_returning"),
            "is_installed": first_present(station, "is_installed"),
        })
    return rows


def vehicle_rows(feed, fetched_at):
    """Flatten free_bike_status.json (v1/v2) or vehicle_status.json (v3)."""
    last_updated = normalise_timestamp(feed.get("last_updated"))
    data = feed.get("data", {})
    vehicles = data.get("bikes") or data.get("vehicles") or []

    rows = []
    for vehicle in vehicles:
        rows.append({
            "fetched_at": fetched_at,
            "last_updated": last_updated,
            "vehicle_id": first_present(vehicle, "bike_id", "vehicle_id"),
            "lat": first_present(vehicle, "lat"),
            "lon": first_present(vehicle, "lon"),
            "is_reserved": first_present(vehicle, "is_reserved"),
            "is_disabled": first_present(vehicle, "is_disabled"),
            "vehicle_type_id": first_present(vehicle, "vehicle_type_id"),
            "current_range_meters": first_present(vehicle, "current_range_meters"),
        })
    return rows


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def append_rows(path, columns, rows):
    """Append rows to a daily CSV, writing the header only on first creation.

    One file per day keeps things small enough to open and eyeball when
    something looks wrong, which it will.
    """
    if not rows:
        return 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    is_new_file = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        if is_new_file:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def compress_old_days(directory, today):
    """Gzip finished daily files. Today's file stays plain so it can be appended.

    Left alone, a 200-station system polled every 10 minutes produces roughly
    2.5 MB of CSV a day, or about 450 MB over six months. That will make a git
    repository unhappy. This data is enormously repetitive — the same station
    IDs and timestamps over and over — so it compresses about 12:1 for free.
    """
    if not os.path.isdir(directory):
        return
    for filename in os.listdir(directory):
        if not filename.endswith(".csv") or filename.startswith(today):
            continue
        source = os.path.join(directory, filename)
        with open(source, "rb") as raw, gzip.open(source + ".gz", "wb") as compressed:
            shutil.copyfileobj(raw, compressed)
        os.remove(source)


def save_snapshot(path, document):
    """Save a whole feed verbatim. Used for the slow-changing files.

    station_information holds names, coordinates and dock capacity. It barely
    changes, but when it does — a station moves, or capacity is expanded —
    that is a natural experiment. Keeping dated copies means you can prove
    what changed and when.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(document, handle, indent=2)
        return True
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Poll a GBFS feed into daily CSVs.")
    parser.add_argument("--url", required=True, help="the system's gbfs.json URL")
    parser.add_argument("--system", required=True, help="short name, used as folder name")
    parser.add_argument("--out", default="data", help="output directory")
    parser.add_argument("--list-feeds", action="store_true",
                        help="print the feeds this system offers, then exit")
    arguments = parser.parse_args()

    feeds = discover_feeds(arguments.url)

    if arguments.list_feeds:
        for name, url in sorted(feeds.items()):
            print(f"{name:<28} {url}")
        return 0

    now = dt.datetime.now(dt.timezone.utc)
    fetched_at = now.isoformat()
    today = now.strftime("%Y-%m-%d")
    base = os.path.join(arguments.out, arguments.system)

    written = []

    # Docked systems: the status file is the one that changes minute to minute.
    if "station_status" in feeds:
        feed = fetch_json(feeds["station_status"])
        count = append_rows(
            os.path.join(base, "stations", f"{today}.csv"),
            STATION_COLUMNS,
            station_rows(feed, fetched_at),
        )
        written.append(f"{count} stations")

    # Dockless systems: individual vehicles, with coordinates.
    dockless_key = next(
        (k for k in ("free_bike_status", "vehicle_status") if k in feeds), None)
    if dockless_key:
        feed = fetch_json(feeds[dockless_key])
        count = append_rows(
            os.path.join(base, "vehicles", f"{today}.csv"),
            VEHICLE_COLUMNS,
            vehicle_rows(feed, fetched_at),
        )
        written.append(f"{count} vehicles")

    # Slow-changing reference files: one dated copy per day is plenty.
    for feed_name in ("station_information", "system_pricing_plans", "system_information"):
        if feed_name in feeds:
            path = os.path.join(base, feed_name, f"{today}.json")
            if not os.path.exists(path):
                save_snapshot(path, fetch_json(feeds[feed_name]))
                written.append(f"{feed_name} snapshot")

    # Roll up yesterday's files now that nothing more will be appended to them.
    for subfolder in ("stations", "vehicles"):
        compress_old_days(os.path.join(base, subfolder), today)

    print(f"{fetched_at}  {arguments.system}  " + ", ".join(written or ["nothing"]))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        # Print and exit non-zero so your scheduler logs the failure loudly,
        # instead of silently recording nothing for a week.
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
