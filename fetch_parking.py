"""
fetch_parking.py — Lädt P+R und B+R Stationen von MobiData BW ParkAPI v3.
Ergebnisse werden als JSON in data/ gecacht.
Fallback: eingebettete kuratierte Liste für Lörrach/Freiburg/Basel.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any, Optional

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

PARKAPI_BASE = "https://api.mobidata-bw.de/park-api/api/public/v3/parking-sites"
TIMEOUT = 30  # seconds
PAGE_SIZE = 500

# ─── Kuratierte Fallback-Liste (Region Lörrach / Freiburg / Basel) ────────────
# Quellen: MobiData BW Datensätze, manuell verifiziert

FALLBACK_PARK_RIDE: list[dict[str, Any]] = [
    {"id": "fb-pr-hbf", "name": "P+R Freiburg Hbf (Süd)", "lat": 47.9958, "lon": 7.8421, "capacity": 220, "purpose": "CAR"},
    {"id": "fb-pr-padua", "name": "P+R Freiburg Paduaallee", "lat": 48.0234, "lon": 7.8312, "capacity": 180, "purpose": "CAR"},
    {"id": "fb-pr-munzinger", "name": "P+R Freiburg-Munzinger Str.", "lat": 47.9712, "lon": 7.8198, "capacity": 95, "purpose": "CAR"},
    {"id": "loe-pr-hbf", "name": "P+R Lörrach Hbf", "lat": 47.6128, "lon": 7.6608, "capacity": 120, "purpose": "CAR"},
    {"id": "loe-pr-stetten", "name": "P+R Lörrach-Stetten", "lat": 47.6201, "lon": 7.6453, "capacity": 60, "purpose": "CAR"},
    {"id": "war-pr-bf", "name": "P+R Weil am Rhein Bf", "lat": 47.5942, "lon": 7.6138, "capacity": 85, "purpose": "CAR"},
    {"id": "bk-pr-bf", "name": "P+R Bad Krozingen Bf", "lat": 47.9186, "lon": 7.7033, "capacity": 110, "purpose": "CAR"},
    {"id": "mh-pr-bf", "name": "P+R Müllheim Bf", "lat": 47.8095, "lon": 7.6281, "capacity": 75, "purpose": "CAR"},
    {"id": "em-pr-bf", "name": "P+R Emmendingen Bf", "lat": 48.1204, "lon": 7.8507, "capacity": 90, "purpose": "CAR"},
    {"id": "fb-pr-littenweiler", "name": "P+R Freiburg-Littenweiler", "lat": 47.9813, "lon": 7.8812, "capacity": 50, "purpose": "CAR"},
    {"id": "dz-pr-bf", "name": "P+R Denzlingen Bf", "lat": 48.0688, "lon": 7.8764, "capacity": 65, "purpose": "CAR"},
    {"id": "gt-pr-bf", "name": "P+R Gundelfingen Bf", "lat": 48.0431, "lon": 7.8654, "capacity": 40, "purpose": "CAR"},
]

FALLBACK_BIKE_RIDE: list[dict[str, Any]] = [
    {"id": "fb-br-hbf", "name": "B+R Freiburg Hbf", "lat": 47.9972, "lon": 7.8413, "capacity": 1200, "purpose": "BIKE", "type": "Fahrradparkhaus"},
    {"id": "fb-br-bertoldsbr", "name": "B+R Bertoldsbrunnen", "lat": 47.9942, "lon": 7.8491, "capacity": 80, "purpose": "BIKE", "type": "Bügel"},
    {"id": "fb-br-stadttheater", "name": "B+R Stadttheater", "lat": 47.9933, "lon": 7.8463, "capacity": 60, "purpose": "BIKE", "type": "Bügel"},
    {"id": "loe-br-hbf", "name": "B+R Lörrach Hbf", "lat": 47.6131, "lon": 7.6612, "capacity": 150, "purpose": "BIKE", "type": "Bügel"},
    {"id": "loe-br-museum", "name": "B+R Lörrach Museum/Bf", "lat": 47.6152, "lon": 7.6587, "capacity": 40, "purpose": "BIKE", "type": "Box"},
    {"id": "war-br-bf", "name": "B+R Weil am Rhein Bf", "lat": 47.5945, "lon": 7.6142, "capacity": 80, "purpose": "BIKE", "type": "Bügel"},
    {"id": "bk-br-bf", "name": "B+R Bad Krozingen Bf", "lat": 47.9189, "lon": 7.7037, "capacity": 120, "purpose": "BIKE", "type": "Bügel"},
    {"id": "mh-br-bf", "name": "B+R Müllheim Bf", "lat": 47.8098, "lon": 7.6285, "capacity": 90, "purpose": "BIKE", "type": "Bügel"},
    {"id": "em-br-bf", "name": "B+R Emmendingen Bf", "lat": 48.1207, "lon": 7.8511, "capacity": 100, "purpose": "BIKE", "type": "Box"},
    {"id": "fb-br-littenweiler", "name": "B+R FR-Littenweiler", "lat": 47.9816, "lon": 7.8815, "capacity": 45, "purpose": "BIKE", "type": "Bügel"},
    {"id": "dz-br-bf", "name": "B+R Denzlingen Bf", "lat": 48.0691, "lon": 7.8768, "capacity": 70, "purpose": "BIKE", "type": "Bügel"},
    {"id": "gt-br-bf", "name": "B+R Gundelfingen Bf", "lat": 48.0434, "lon": 7.8658, "capacity": 55, "purpose": "BIKE", "type": "Bügel"},
]


def _fetch_page(purpose: str, offset: int) -> Optional[dict]:
    """Fetch one page of parking sites from ParkAPI v3."""
    params = {
        "purpose": purpose,
        "limit": PAGE_SIZE,
        "offset": offset,
    }
    try:
        resp = httpx.get(PARKAPI_BASE, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except (httpx.HTTPError, httpx.TimeoutException, json.JSONDecodeError) as exc:
        log.warning("ParkAPI request failed (purpose=%s, offset=%d): %s", purpose, offset, exc)
        return None


def _parse_site(raw: dict) -> Optional[dict]:
    """Extract relevant fields from a ParkAPI parking-site object."""
    lat = raw.get("lat") or raw.get("coordinates", {}).get("lat")
    lon = raw.get("lon") or raw.get("coordinates", {}).get("lon")
    if lat is None or lon is None:
        return None
    try:
        lat = float(lat)
        lon = float(lon)
    except (ValueError, TypeError):
        return None
    # Filter to Baden-Württemberg rough bounding box
    if not (47.5 <= lat <= 49.8 and 7.5 <= lon <= 10.5):
        return None
    site: dict[str, Any] = {
        "id": str(raw.get("uid", raw.get("id", ""))),
        "name": raw.get("name", "Unbekannt"),
        "lat": float(lat),
        "lon": float(lon),
        "capacity": raw.get("capacity") or raw.get("max_capacity") or 0,
        "purpose": raw.get("purpose", "CAR"),
    }
    # Realtime availability
    available = raw.get("realtime_free_capacity") or raw.get("available_spaces")
    if available is not None:
        site["available"] = int(available)
        site["has_realtime"] = True
    else:
        site["has_realtime"] = False
    # B+R type
    if raw.get("purpose") == "BIKE":
        site["type"] = raw.get("type", raw.get("facility_type", "Bügel"))
    return site


def fetch_parking(purpose: str) -> list[dict[str, Any]]:
    """
    Fetch all parking sites for the given purpose (CAR or BIKE).
    Paginates through ParkAPI v3. Falls back to embedded list on failure.
    """
    label = "P+R" if purpose == "CAR" else "B+R"
    log.info("Fetching %s stations from ParkAPI v3 (purpose=%s)...", label, purpose)

    all_sites: list[dict[str, Any]] = []
    offset = 0
    api_ok = True

    while True:
        page = _fetch_page(purpose, offset)
        if page is None:
            api_ok = False
            break

        # ParkAPI v3 returns items in different possible structures
        items = page if isinstance(page, list) else page.get("items", page.get("data", []))
        if not items:
            break

        for raw in items:
            site = _parse_site(raw)
            if site:
                all_sites.append(site)

        if len(items) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.3)  # Rate-limit courtesy

    if all_sites:
        source = "API"
    else:
        # Fallback
        api_ok = False
        all_sites = FALLBACK_PARK_RIDE if purpose == "CAR" else FALLBACK_BIKE_RIDE
        for s in all_sites:
            s.setdefault("has_realtime", False)
        source = "FALLBACK"

    # Stats
    with_realtime = sum(1 for s in all_sites if s.get("has_realtime"))
    without_realtime = len(all_sites) - with_realtime
    log.info(
        "%s stations loaded: %d total (source: %s, realtime: %d, no-realtime: %d)",
        label, len(all_sites), source, with_realtime, without_realtime,
    )

    return all_sites


def main() -> None:
    """Fetch P+R and B+R data, cache as JSON."""
    park_ride = fetch_parking("CAR")
    bike_ride = fetch_parking("BIKE")

    pr_path = DATA_DIR / "park_ride.json"
    br_path = DATA_DIR / "bike_ride.json"

    with open(pr_path, "w", encoding="utf-8") as f:
        json.dump(park_ride, f, ensure_ascii=False, indent=2)
    log.info("P+R data saved to %s", pr_path)

    with open(br_path, "w", encoding="utf-8") as f:
        json.dump(bike_ride, f, ensure_ascii=False, indent=2)
    log.info("B+R data saved to %s", br_path)


if __name__ == "__main__":
    main()
