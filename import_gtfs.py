"""
import_gtfs.py — Importiert GTFS-Daten und P+R/B+R-Stationen in Neo4j.

Ablauf:
1. GTFS-ZIP herunterladen (oder lokal lesen)
2. stops, stop_times, trips, routes, calendar/calendar_dates parsen
3. Neo4j-Graph aufbauen: Stop-Nodes, NEXT_STOP-Edges, TRANSFER-Edges
4. P+R- und B+R-Nodes einfügen und mit nächsten Stops verknüpfen
5. Validierung + Statistiken ausgeben

Verwendung:
    python import_gtfs.py                           # Download von MobiData BW
    python import_gtfs.py --gtfs-path data/gtfs.zip # Lokale ZIP-Datei
    python import_gtfs.py --gtfs-dir data/gtfs/     # Bereits entpacktes Verzeichnis
"""

import argparse
import csv
import io
import json
import logging
import math
import os
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import httpx
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

GTFS_URL = "https://www.mobidata-bw.de/dataset/9cd6dabb-fcac-4aae-9bc4-6a9b6e492465/resource/bfab2e40-a611-4bfe-8499-ee745da1e322/download/gtfs-bw.zip"
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASS", "mobidata2024")

# BW bounding box for filtering (with margin)
BW_LAT_MIN, BW_LAT_MAX = 47.4, 49.9
BW_LON_MIN, BW_LON_MAX = 7.4, 10.6

# Focus region: Lörrach/Freiburg/Basel corridor for manageable graph size
FOCUS_LAT_MIN, FOCUS_LAT_MAX = 47.5, 48.3
FOCUS_LON_MIN, FOCUS_LON_MAX = 7.5, 8.1

TRANSFER_MAX_M = 300       # Max walking distance for transfers between stops
PR_LINK_MAX_M = 800        # Max distance P+R → Stop
BR_LINK_MAX_M = 300        # Max distance B+R → Stop
WALK_SPEED_KMH = 4.5       # Walking speed for time estimates


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance between two points in meters."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def walk_minutes(distance_m: float) -> float:
    """Walking time in minutes at WALK_SPEED_KMH."""
    return (distance_m / 1000) / WALK_SPEED_KMH * 60


def time_to_minutes(t: str) -> int:
    """Convert GTFS time string HH:MM:SS to minutes since midnight.
    GTFS allows hours > 23 for trips past midnight."""
    parts = t.strip().split(":")
    return int(parts[0]) * 60 + int(parts[1])


# ─── GTFS Parsing ─────────────────────────────────────────────────────────────

def download_gtfs(dest: Path) -> Path:
    """Download GTFS ZIP from MobiData BW."""
    if dest.exists():
        log.info("GTFS file already exists at %s, skipping download.", dest)
        return dest
    log.info("Downloading GTFS from %s ...", GTFS_URL)
    with httpx.stream("GET", GTFS_URL, follow_redirects=True, timeout=300) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(dest, "wb") as f:
            for chunk in resp.iter_bytes(chunk_size=1024 * 256):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    if int(pct) % 10 == 0:
                        log.info("  %.0f%% downloaded (%d MB)", pct, downloaded // (1024 * 1024))
    log.info("GTFS saved to %s (%.1f MB)", dest, dest.stat().st_size / (1024 * 1024))
    return dest


def read_csv_from_zip(zf: zipfile.ZipFile, filename: str) -> list[dict[str, str]]:
    """Read a CSV file from a ZIP archive, return list of dicts."""
    try:
        with zf.open(filename) as f:
            text = io.TextIOWrapper(f, encoding="utf-8-sig")
            reader = csv.DictReader(text)
            return list(reader)
    except KeyError:
        log.warning("File %s not found in GTFS ZIP.", filename)
        return []


def read_csv_from_dir(dir_path: Path, filename: str) -> list[dict[str, str]]:
    """Read a CSV file from a directory."""
    fpath = dir_path / filename
    if not fpath.exists():
        log.warning("File %s not found in %s.", filename, dir_path)
        return []
    with open(fpath, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


class GTFSData:
    """Parsed GTFS data filtered to focus region."""

    def __init__(self):
        self.stops: dict[str, dict] = {}           # stop_id → {name, lat, lon}
        self.connections: list[dict] = []           # Sorted stop_time pairs
        self.active_services: set[str] = set()      # Active service_ids
        self.route_info: dict[str, dict] = {}       # route_id → {name, type}

    def parse(self, reader_fn) -> None:
        """Parse GTFS data using the given reader function.
        reader_fn(filename) → list[dict]"""

        t0 = time.time()

        # 1. Routes
        for row in reader_fn("routes.txt"):
            self.route_info[row["route_id"]] = {
                "name": row.get("route_short_name") or row.get("route_long_name", ""),
                "type": int(row.get("route_type", 3)),
            }
        log.info("Parsed %d routes.", len(self.route_info))

        # 2. Calendar / Calendar Dates — determine active service IDs
        self._parse_services(reader_fn)
        if self.active_services is not None:
            log.info("Active services: %d", len(self.active_services))
        else:
            log.info("Active services: ALL (no calendar data found)")

        # 3. Trips — map trip_id to route_id, filter by active services
        trips: dict[str, str] = {}  # trip_id → route_id
        all_trips = reader_fn("trips.txt")
        skipped = 0
        for row in all_trips:
            # If active_services is None (sentinel), accept all trips
            if self.active_services is None or row.get("service_id") in self.active_services:
                trips[row["trip_id"]] = row["route_id"]
            else:
                skipped += 1
        pct_skipped = (skipped / len(all_trips) * 100) if all_trips else 0
        log.info("Trips: %d active, %d skipped (%.1f%% no active service).",
                 len(trips), skipped, pct_skipped)
        if pct_skipped > 5:
            log.warning("⚠ %.1f%% of trips have no active service — check calendar data!", pct_skipped)

        # 4. Stops — filter to focus region
        all_stops = reader_fn("stops.txt")
        for row in all_stops:
            lat = float(row.get("stop_lat", 0))
            lon = float(row.get("stop_lon", 0))
            if FOCUS_LAT_MIN <= lat <= FOCUS_LAT_MAX and FOCUS_LON_MIN <= lon <= FOCUS_LON_MAX:
                self.stops[row["stop_id"]] = {
                    "name": row.get("stop_name", ""),
                    "lat": lat,
                    "lon": lon,
                }
        log.info("Stops in focus region: %d (of %d total).", len(self.stops), len(all_stops))

        # 5. Stop Times — build connections between consecutive stops on a trip
        stop_times_raw = reader_fn("stop_times.txt")
        # Group by trip_id, only active trips and stops in focus region
        by_trip: dict[str, list[tuple[int, str, str, str]]] = defaultdict(list)
        for row in stop_times_raw:
            tid = row["trip_id"]
            sid = row["stop_id"]
            if tid in trips and sid in self.stops:
                seq = int(row.get("stop_sequence", 0))
                dep = row.get("departure_time", "")
                arr = row.get("arrival_time", "")
                if dep and arr:
                    by_trip[tid].append((seq, sid, dep, arr))

        for tid, st_list in by_trip.items():
            st_list.sort(key=lambda x: x[0])
            route_id = trips[tid]
            for i in range(len(st_list) - 1):
                _, s1, _, dep1 = st_list[i]
                _, s2, arr2, _ = st_list[i + 1]
                dep_min = time_to_minutes(dep1)
                arr_min = time_to_minutes(arr2)
                duration = arr_min - dep_min
                if duration < 0:
                    duration += 24 * 60  # Midnight wrap
                if duration > 180:
                    continue  # Skip implausible connections (>3h between consecutive stops)
                self.connections.append({
                    "from": s1,
                    "to": s2,
                    "trip_id": tid,
                    "route_id": route_id,
                    "departure": dep_min,
                    "arrival": arr_min,
                    "duration": duration,
                })

        log.info("Connections: %d (parsed in %.1fs).", len(self.connections), time.time() - t0)

    def _parse_services(self, reader_fn) -> None:
        """Determine active service IDs from calendar.txt and/or calendar_dates.txt.
        Handles three cases:
        1. calendar.txt exists → use it as base
        2. Only calendar_dates.txt → extract service_ids from exception_type=1
        3. Both → calendar.txt as base, calendar_dates.txt as override
        """
        calendar_rows = reader_fn("calendar.txt")
        calendar_dates_rows = reader_fn("calendar_dates.txt")

        if calendar_rows:
            # Case 1 or 3: calendar.txt exists
            # Accept all services that run on at least one weekday
            # (For MVP we accept all services regardless of date range —
            #  a production system would check date ranges and day-of-week)
            for row in calendar_rows:
                weekdays = [row.get(d, "0") for d in
                            ["monday", "tuesday", "wednesday", "thursday", "friday"]]
                if any(d == "1" for d in weekdays):
                    self.active_services.add(row["service_id"])
            log.info("calendar.txt: %d weekday services.", len(self.active_services))

        if calendar_dates_rows:
            added = 0
            removed = 0
            for row in calendar_dates_rows:
                sid = row.get("service_id", "")
                exc_type = row.get("exception_type", "")
                if exc_type == "1":
                    self.active_services.add(sid)
                    added += 1
                elif exc_type == "2":
                    self.active_services.discard(sid)
                    removed += 1
            log.info("calendar_dates.txt: %d added, %d removed.", added, removed)

        if not calendar_rows and not calendar_dates_rows:
            log.warning("⚠ Neither calendar.txt nor calendar_dates.txt found! "
                        "Accepting ALL trips as active.")
            self.active_services = None  # type: ignore — sentinel: accept all


# ─── Neo4j Import ─────────────────────────────────────────────────────────────

class GraphBuilder:
    """Builds the Neo4j graph from parsed GTFS + parking data."""

    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self._verify_connection()

    def _verify_connection(self) -> None:
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS n")
            assert result.single()["n"] == 1
        log.info("Connected to Neo4j at %s.", NEO4J_URI)

    def close(self) -> None:
        self.driver.close()

    def clear_graph(self) -> None:
        """Remove all existing nodes and relationships."""
        with self.driver.session() as s:
            s.run("MATCH (n) DETACH DELETE n")
        log.info("Graph cleared.")

    def create_indexes(self) -> None:
        """Create indexes for fast lookups."""
        with self.driver.session() as s:
            s.run("CREATE INDEX stop_id_idx IF NOT EXISTS FOR (s:Stop) ON (s.stop_id)")
            s.run("CREATE INDEX pr_id_idx IF NOT EXISTS FOR (p:ParkRide) ON (p.id)")
            s.run("CREATE INDEX br_id_idx IF NOT EXISTS FOR (b:BikeRide) ON (b.id)")
            s.run("CREATE INDEX stop_loc_idx IF NOT EXISTS FOR (s:Stop) ON (s.lat, s.lon)")
        log.info("Indexes created.")

    def import_stops(self, stops: dict[str, dict]) -> int:
        """Create Stop nodes in batches."""
        batch_size = 500
        items = list(stops.items())
        count = 0
        with self.driver.session() as s:
            for i in range(0, len(items), batch_size):
                batch = [
                    {"stop_id": sid, "name": d["name"], "lat": d["lat"], "lon": d["lon"]}
                    for sid, d in items[i:i + batch_size]
                ]
                s.run(
                    """UNWIND $batch AS row
                    CREATE (s:Stop {
                        stop_id: row.stop_id,
                        name: row.name,
                        lat: row.lat,
                        lon: row.lon
                    })""",
                    batch=batch,
                )
                count += len(batch)
        log.info("Imported %d Stop nodes.", count)
        return count

    def import_connections(self, connections: list[dict], route_info: dict) -> int:
        """Create NEXT_STOP relationships in batches."""
        batch_size = 2000
        count = 0
        with self.driver.session() as s:
            for i in range(0, len(connections), batch_size):
                batch = []
                for c in connections[i:i + batch_size]:
                    ri = route_info.get(c["route_id"], {})
                    batch.append({
                        "from_id": c["from"],
                        "to_id": c["to"],
                        "trip_id": c["trip_id"],
                        "route_id": c["route_id"],
                        "departure": c["departure"],
                        "arrival": c["arrival"],
                        "duration": c["duration"],
                        "route_name": ri.get("name", ""),
                        "route_type": ri.get("type", 3),
                    })
                s.run(
                    """UNWIND $batch AS row
                    MATCH (a:Stop {stop_id: row.from_id})
                    MATCH (b:Stop {stop_id: row.to_id})
                    CREATE (a)-[:NEXT_STOP {
                        trip_id: row.trip_id,
                        route_id: row.route_id,
                        departure: row.departure,
                        arrival: row.arrival,
                        duration_min: row.duration,
                        route_name: row.route_name,
                        route_type: row.route_type
                    }]->(b)""",
                    batch=batch,
                )
                count += len(batch)
        log.info("Imported %d NEXT_STOP relationships.", count)
        return count

    def create_transfers(self, stops: dict[str, dict]) -> int:
        """Create TRANSFER relationships between stops within TRANSFER_MAX_M.
        Uses a spatial approach: sort by lat, only compare nearby stops."""
        stop_list = [(sid, d["lat"], d["lon"]) for sid, d in stops.items()]
        stop_list.sort(key=lambda x: x[1])  # Sort by latitude

        transfers: list[dict] = []
        n = len(stop_list)
        for i in range(n):
            sid1, lat1, lon1 = stop_list[i]
            for j in range(i + 1, n):
                sid2, lat2, lon2 = stop_list[j]
                # Early exit: if lat difference exceeds ~400m, no point checking further
                if (lat2 - lat1) > 0.004:
                    break
                dist = haversine_m(lat1, lon1, lat2, lon2)
                if dist <= TRANSFER_MAX_M and dist > 10:  # >10m to avoid self-links
                    wmin = round(walk_minutes(dist), 1)
                    transfers.append({
                        "from_id": sid1, "to_id": sid2,
                        "distance_m": round(dist), "walk_min": wmin,
                    })
                    transfers.append({
                        "from_id": sid2, "to_id": sid1,
                        "distance_m": round(dist), "walk_min": wmin,
                    })

        # Batch import
        batch_size = 2000
        with self.driver.session() as s:
            for i in range(0, len(transfers), batch_size):
                s.run(
                    """UNWIND $batch AS row
                    MATCH (a:Stop {stop_id: row.from_id})
                    MATCH (b:Stop {stop_id: row.to_id})
                    CREATE (a)-[:TRANSFER {
                        distance_m: row.distance_m,
                        walk_min: row.walk_min
                    }]->(b)""",
                    batch=transfers[i:i + batch_size],
                )
        log.info("Created %d TRANSFER relationships.", len(transfers))
        return len(transfers)

    def import_parking(self, stations: list[dict], label: str, max_dist_m: float) -> int:
        """Import P+R or B+R stations and link to nearby Stops."""
        if not stations:
            log.warning("No %s stations to import.", label)
            return 0

        # Create station nodes
        with self.driver.session() as s:
            for st in stations:
                props = {
                    "id": st["id"],
                    "name": st["name"],
                    "lat": st["lat"],
                    "lon": st["lon"],
                    "capacity": st.get("capacity", 0),
                    "has_realtime": st.get("has_realtime", False),
                    "source": st.get("source", "api"),
                }
                if "available" in st:
                    props["available"] = st["available"]
                if "type" in st:
                    props["type"] = st["type"]

                s.run(f"CREATE (n:{label} $props)", props=props)

        # Link to nearby stops
        link_count = 0
        with self.driver.session() as s:
            result = s.run(f"MATCH (p:{label}) RETURN p.id AS id, p.lat AS lat, p.lon AS lon")
            park_nodes = [(r["id"], r["lat"], r["lon"]) for r in result]

            result = s.run("MATCH (s:Stop) RETURN s.stop_id AS id, s.lat AS lat, s.lon AS lon")
            stop_nodes = [(r["id"], r["lat"], r["lon"]) for r in result]

        links: list[dict] = []
        for pid, plat, plon in park_nodes:
            for sid, slat, slon in stop_nodes:
                dist = haversine_m(plat, plon, slat, slon)
                if dist <= max_dist_m:
                    links.append({
                        "park_id": pid,
                        "stop_id": sid,
                        "distance_m": round(dist),
                        "walk_min": round(walk_minutes(dist), 1),
                    })

        with self.driver.session() as s:
            for i in range(0, len(links), 500):
                s.run(
                    f"""UNWIND $batch AS row
                    MATCH (p:{label} {{id: row.park_id}})
                    MATCH (s:Stop {{stop_id: row.stop_id}})
                    CREATE (p)-[:NEAR_STOP {{
                        distance_m: row.distance_m,
                        walk_min: row.walk_min
                    }}]->(s)""",
                    batch=links[i:i + 500],
                )
        log.info("Imported %d %s stations with %d NEAR_STOP links.", len(stations), label, len(links))
        return len(links)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Import GTFS + Parking into Neo4j")
    parser.add_argument("--gtfs-path", type=Path, help="Path to GTFS ZIP file")
    parser.add_argument("--gtfs-dir", type=Path, help="Path to extracted GTFS directory")
    parser.add_argument("--no-download", action="store_true", help="Skip download, use existing file")
    parser.add_argument("--clear", action="store_true", default=True, help="Clear graph before import")
    args = parser.parse_args()

    t_start = time.time()

    # Determine GTFS source
    if args.gtfs_dir:
        reader_fn = lambda fname: read_csv_from_dir(args.gtfs_dir, fname)
        log.info("Reading GTFS from directory: %s", args.gtfs_dir)
    else:
        zip_path = args.gtfs_path or DATA_DIR / "gtfs-bw.zip"
        if not zip_path.exists() and not args.no_download:
            download_gtfs(zip_path)
        if not zip_path.exists():
            log.error("GTFS file not found at %s. Download first or provide --gtfs-path.", zip_path)
            return
        zf = zipfile.ZipFile(zip_path)
        reader_fn = lambda fname, _zf=zf: read_csv_from_zip(_zf, fname)
        log.info("Reading GTFS from ZIP: %s", zip_path)

    # Parse GTFS
    gtfs = GTFSData()
    gtfs.parse(reader_fn)

    if not gtfs.stops:
        log.error("No stops found in focus region! Check coordinates or GTFS data.")
        return

    # Load parking data
    pr_path = DATA_DIR / "park_ride.json"
    br_path = DATA_DIR / "bike_ride.json"
    park_ride: list[dict] = []
    bike_ride: list[dict] = []

    if pr_path.exists():
        with open(pr_path, encoding="utf-8") as f:
            park_ride = json.load(f)
        log.info("Loaded %d P+R stations from cache.", len(park_ride))
    else:
        log.warning("P+R data not found at %s. Run fetch_parking.py first.", pr_path)

    if br_path.exists():
        with open(br_path, encoding="utf-8") as f:
            bike_ride = json.load(f)
        log.info("Loaded %d B+R stations from cache.", len(bike_ride))
    else:
        log.warning("B+R data not found at %s. Run fetch_parking.py first.", br_path)

    # Build graph
    builder = GraphBuilder(NEO4J_URI, NEO4J_USER, NEO4J_PASS)
    try:
        if args.clear:
            builder.clear_graph()
        builder.create_indexes()

        n_stops = builder.import_stops(gtfs.stops)
        n_conn = builder.import_connections(gtfs.connections, gtfs.route_info)
        n_transfers = builder.create_transfers(gtfs.stops)
        n_pr_links = builder.import_parking(park_ride, "ParkRide", PR_LINK_MAX_M)
        n_br_links = builder.import_parking(bike_ride, "BikeRide", BR_LINK_MAX_M)

        elapsed = time.time() - t_start
        log.info("═══ Import complete in %.1fs ═══", elapsed)
        log.info("  Stops:       %d", n_stops)
        log.info("  Connections: %d", n_conn)
        log.info("  Transfers:   %d", n_transfers)
        log.info("  P+R links:   %d", n_pr_links)
        log.info("  B+R links:   %d", n_br_links)
    finally:
        builder.close()


if __name__ == "__main__":
    main()
