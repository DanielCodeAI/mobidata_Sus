"""
app.py — FastAPI Backend für das multimodale Pendler-Routing-Tool.

Endpoints:
  POST /route          — Berechnet Auto / P+R / B+R Routen
  GET  /benchmark      — Algorithmen-Vergleich über n zufällige Paare
  GET  /score-sensitivity — Pendler-Score mit 10 Gewichtungskombinationen
  GET  /stops          — Alle Haltestellen (für Frontend-Karte)
  GET  /parking        — Alle P+R/B+R Stationen
  GET  /health         — Health-Check

Starten:  uvicorn app:app --reload --port 8000
"""

import json
import logging
import math
import os
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from neo4j import GraphDatabase

from routing import (
    TransitGraph, GreedyRouter, DijkstraRouter, AStarRouter,
    RoutingResult, run_all_algorithms, run_algorithm,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data"
STATIC_DIR = Path(__file__).parent / "static"

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASS", "mobidata2024")

# ─── CO2 & Cost Constants ────────────────────────────────────────────────────
# Pkw: 152 g CO2/km (Quelle: UBA 2024, "Emissionsdaten Personenverkehr",
#       deutscher Pkw-Durchschnitt inkl. Vorkette)
CO2_CAR_G_PER_KM = 152

# ÖPNV: 55 g CO2/Personenkilometer (Quelle: UBA 2024, Nahverkehr Bus/Bahn Mix)
CO2_TRANSIT_G_PER_KM = 55

# Fahrrad / Fußweg: 0 g CO2/km
CO2_BIKE_G_PER_KM = 0
CO2_WALK_G_PER_KM = 0

# Spritkosten: 1.75 €/L Benzin (Quelle: ADAC Durchschnitt Deutschland 2024)
# Verbrauch: 7.4 L/100km (Quelle: KBA, deutscher Pkw-Durchschnitt 2023)
FUEL_PRICE_EUR_PER_L = 1.75
FUEL_CONSUMPTION_L_PER_100KM = 7.4
CAR_COST_EUR_PER_KM = FUEL_PRICE_EUR_PER_L * FUEL_CONSUMPTION_L_PER_100KM / 100

# ÖPNV Monatskarte Freiburg/Lörrach Zone: ~80€, bei 20 Tagen = 4€/Tag = 2€ pro Fahrt
TRANSIT_COST_PER_TRIP_EUR = 2.0

# Fahrrad: Wartung ~200€/Jahr bei 220 Tagen = 0.91€/Tag
BIKE_COST_PER_TRIP_EUR = 0.91

# OSRM public demo API (kein Key nötig, Rate-Limit beachten)
OSRM_BASE = "https://router.project-osrm.org/route/v1/driving"

# Nominatim geocoding (OSM, kostenlos)
NOMINATIM_BASE = "https://nominatim.openstreetmap.org/search"

# Detour-Index für Haversine-Fallback (Quelle: Boscoe et al. 2012,
# "A nationwide comparison of driving distance vs straight-line distance")
DETOUR_INDEX_CAR = 1.4
DETOUR_INDEX_BIKE = 1.3

WALK_SPEED_KMH = 4.5
BIKE_SPEED_KMH = 15.0

# ─── Global State ─────────────────────────────────────────────────────────────

transit_graph: Optional[TransitGraph] = None
park_ride_stations: list[dict] = []
bike_ride_stations: list[dict] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load transit graph and parking data on startup."""
    global transit_graph, park_ride_stations, bike_ride_stations
    log.info("Loading transit graph from Neo4j...")
    try:
        transit_graph = TransitGraph()
        transit_graph.load_from_neo4j()
        log.info("Graph loaded: %d stops, %d connections.",
                 transit_graph.n_stops, transit_graph.n_connections)
    except Exception as e:
        log.error("Failed to load transit graph: %s", e)
        log.warning("Starting without transit graph — ÖPNV routing will be unavailable.")
        transit_graph = None

    # Load cached parking data
    pr_path = DATA_DIR / "park_ride.json"
    br_path = DATA_DIR / "bike_ride.json"
    if pr_path.exists():
        park_ride_stations = json.loads(pr_path.read_text(encoding="utf-8"))
        log.info("Loaded %d P+R stations.", len(park_ride_stations))
    if br_path.exists():
        bike_ride_stations = json.loads(br_path.read_text(encoding="utf-8"))
        log.info("Loaded %d B+R stations.", len(bike_ride_stations))

    yield


app = FastAPI(title="MobiData Pendler-Routing", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── Helper Functions ─────────────────────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


async def geocode(address: str) -> Optional[tuple]:
    """Geocode an address using Nominatim. Returns (lat, lon) or None."""
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(NOMINATIM_BASE, params={
                "q": address,
                "format": "json",
                "limit": 1,
                "countrycodes": "de,ch",
            }, headers={"User-Agent": "MobiData-Pendler-MVP/1.0"},
            timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception as e:
            log.warning("Geocoding failed for '%s': %s", address, e)
    return None


async def osrm_route(lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[dict]:
    """Get driving route from OSRM. Returns {distance_km, duration_min} or None."""
    url = f"{OSRM_BASE}/{lon1},{lat1};{lon2},{lat2}"
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(url, params={"overview": "full", "geometries": "geojson"},
                                    timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") == "Ok" and data.get("routes"):
                r = data["routes"][0]
                return {
                    "distance_km": round(r["distance"] / 1000, 2),
                    "duration_min": round(r["duration"] / 60, 1),
                    "geometry": r.get("geometry"),
                    "estimated": False,
                }
        except Exception as e:
            log.warning("OSRM request failed: %s", e)
    # Fallback: Haversine with detour index
    dist_km = haversine_km(lat1, lon1, lat2, lon2) * DETOUR_INDEX_CAR
    # At 60 km/h average: 1 km = 1 minute
    duration_min = dist_km
    return {
        "distance_km": round(dist_km, 2),
        "duration_min": round(duration_min, 1),
        "geometry": None,
        "estimated": True,
    }


def find_nearest_stations(lat: float, lon: float,
                          stations: list[dict], max_km: float,
                          limit: int = 5) -> list[dict]:
    """Find nearest parking/bike stations within max_km."""
    results = []
    for s in stations:
        dist = haversine_km(lat, lon, s["lat"], s["lon"])
        if dist <= max_km:
            results.append({**s, "distance_km": round(dist, 2)})
    results.sort(key=lambda x: x["distance_km"])
    return results[:limit]


def find_nearest_stop(lat: float, lon: float, max_km: float = 1.5) -> Optional[dict]:
    """Find nearest transit stop to coordinates."""
    if not transit_graph:
        return None
    best = None
    best_dist = max_km
    for sid, (slat, slon) in transit_graph.stop_coords.items():
        d = haversine_km(lat, lon, slat, slon)
        if d < best_dist:
            best_dist = d
            best = {"stop_id": sid, "name": transit_graph.stop_names[sid],
                    "lat": slat, "lon": slon, "distance_km": round(d, 3)}
    return best


def compute_score(co2_car: float, co2_alt: float,
                  time_car: float, time_alt: float,
                  cost_car: float, cost_alt: float,
                  w_co2: float = 0.4, w_time: float = 0.35,
                  w_cost: float = 0.25) -> float:
    """Compute Pendler-Score (0-100).
    Higher = better alternative to driving."""
    # CO2 score: 100 if alt saves all CO2, 0 if no savings
    if co2_car > 0:
        co2_score = max(0, min(100, (co2_car - co2_alt) / co2_car * 100))
    else:
        co2_score = 50

    # Time score: 100 if alt is faster, 0 if alt takes 2x as long
    if time_car > 0:
        time_ratio = time_alt / time_car
        time_score = max(0, min(100, (2 - time_ratio) / 2 * 100))
    else:
        time_score = 50

    # Cost score: 100 if alt is free, 0 if same cost
    if cost_car > 0:
        cost_score = max(0, min(100, (cost_car - cost_alt) / cost_car * 100))
    else:
        cost_score = 50

    total = w_co2 * co2_score + w_time * time_score + w_cost * cost_score
    # Normalize weights
    w_sum = w_co2 + w_time + w_cost
    if w_sum > 0:
        total = total / w_sum
    return round(max(0, min(100, total)), 1)


def time_str_to_minutes(t: str) -> int:
    """Convert HH:MM to minutes since midnight."""
    parts = t.strip().split(":")
    return int(parts[0]) * 60 + int(parts[1])


def minutes_to_str(m: int) -> str:
    """Convert minutes since midnight to HH:MM."""
    return f"{m // 60:02d}:{m % 60:02d}"


# ─── Request/Response Models ─────────────────────────────────────────────────

class RouteRequest(BaseModel):
    start_lat: Optional[float] = None
    start_lon: Optional[float] = None
    end_lat: Optional[float] = None
    end_lon: Optional[float] = None
    start_address: Optional[str] = None
    end_address: Optional[str] = None
    arrival_time: str = "08:30"
    weight_co2: float = Field(default=0.4, ge=0, le=1)
    weight_time: float = Field(default=0.35, ge=0, le=1)
    weight_cost: float = Field(default=0.25, ge=0, le=1)
    max_bike_km: float = Field(default=5.0, ge=0, le=30)
    max_walk_m: float = Field(default=800.0, ge=0, le=3000)
    algorithm: str = Field(default="astar", description="Routing algorithm: astar, dijkstra, greedy")


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/")
async def serve_frontend():
    index = Path(__file__).parent / "index.html"
    if index.exists():
        return FileResponse(index)
    return JSONResponse({"error": "index.html not found"}, status_code=404)


@app.get("/health")
async def health():
    graph_ok = transit_graph is not None and transit_graph.n_stops > 0
    return {
        "status": "ok" if graph_ok else "degraded",
        "graph_loaded": graph_ok,
        "stops": transit_graph.n_stops if transit_graph else 0,
        "connections": transit_graph.n_connections if transit_graph else 0,
        "park_ride_stations": len(park_ride_stations),
        "bike_ride_stations": len(bike_ride_stations),
    }


@app.post("/route")
async def compute_route(req: RouteRequest):
    """Main routing endpoint: computes car-only, P+R, and B+R options."""
    # Resolve coordinates
    start_lat, start_lon = req.start_lat, req.start_lon
    end_lat, end_lon = req.end_lat, req.end_lon

    if start_lat is None and req.start_address:
        coords = await geocode(req.start_address)
        if not coords:
            raise HTTPException(400, f"Startadresse nicht gefunden: {req.start_address}")
        start_lat, start_lon = coords

    if end_lat is None and req.end_address:
        coords = await geocode(req.end_address)
        if not coords:
            raise HTTPException(400, f"Zieladresse nicht gefunden: {req.end_address}")
        end_lat, end_lon = coords

    if start_lat is None or end_lat is None:
        raise HTTPException(400, "Start- und Zielkoordinaten oder -adressen erforderlich.")

    arrival_min = time_str_to_minutes(req.arrival_time)

    # ═══ OPTION 1: Car Only (Baseline) ═══
    car = await osrm_route(start_lat, start_lon, end_lat, end_lon)
    car_co2 = car["distance_km"] * CO2_CAR_G_PER_KM
    car_cost = car["distance_km"] * CAR_COST_EUR_PER_KM
    car_only = {
        "distance_km": car["distance_km"],
        "duration_min": car["duration_min"],
        "co2_g": round(car_co2),
        "cost_eur": round(car_cost, 2),
        "geometry": car.get("geometry"),
        "estimated": car["estimated"],
    }

    # ═══ OPTION 2: Park & Ride ═══
    park_and_ride = None
    pr_reason = None

    nearest_pr = find_nearest_stations(start_lat, start_lon, park_ride_stations, max_km=30, limit=3)
    if not nearest_pr:
        pr_reason = "Keine P+R-Station im Umkreis von 30km gefunden."
    elif not transit_graph or transit_graph.n_stops == 0:
        pr_reason = "ÖPNV-Daten nicht verfügbar."
    else:
        park_and_ride = await _compute_pr_route(
            start_lat, start_lon, end_lat, end_lon,
            nearest_pr, arrival_min, car_only, req,
        )
        if park_and_ride is None:
            pr_reason = (f"Keine ÖPNV-Verbindung im Zeitfenster "
                         f"{minutes_to_str(max(0, arrival_min - 60))}-"
                         f"{minutes_to_str(arrival_min)} gefunden.")

    # ═══ OPTION 3: Auto + ÖPNV (nächste Haltestelle, ohne P+R) ═══
    auto_transit = None
    at_reason = None

    if not transit_graph or transit_graph.n_stops == 0:
        at_reason = "ÖPNV-Daten nicht verfügbar."
    else:
        auto_transit = await _compute_auto_transit_route(
            start_lat, start_lon, end_lat, end_lon,
            arrival_min, car_only, req,
        )
        if auto_transit is None:
            at_reason = (f"Keine ÖPNV-Verbindung im Zeitfenster "
                         f"{minutes_to_str(max(0, arrival_min - 60))}-"
                         f"{minutes_to_str(arrival_min)} gefunden.")

    # ═══ OPTION 4: Bike & Ride ═══
    bike_and_ride = None
    br_reason = None

    nearest_br = find_nearest_stations(start_lat, start_lon, bike_ride_stations, max_km=req.max_bike_km, limit=3)
    if not nearest_br:
        br_reason = "Keine B+R-Abstellanlage im Umkreis von 10km gefunden."
    elif not transit_graph or transit_graph.n_stops == 0:
        br_reason = "ÖPNV-Daten nicht verfügbar."
    else:
        bike_and_ride = await _compute_br_route(
            start_lat, start_lon, end_lat, end_lon,
            nearest_br, arrival_min, car_only, req,
        )
        if bike_and_ride is None:
            br_reason = (f"Keine ÖPNV-Verbindung im Zeitfenster "
                         f"{minutes_to_str(max(0, arrival_min - 60))}-"
                         f"{minutes_to_str(arrival_min)} gefunden.")

    # Build response
    response: dict[str, Any] = {
        "car_only": car_only,
        "park_and_ride": park_and_ride,
        "auto_transit": auto_transit,
        "bike_and_ride": bike_and_ride,
        "co2_saved_pr_g": round(car_co2 - park_and_ride["co2_g"]) if park_and_ride else None,
        "co2_saved_at_g": round(car_co2 - auto_transit["co2_g"]) if auto_transit else None,
        "co2_saved_br_g": round(car_co2 - bike_and_ride["co2_g"]) if bike_and_ride else None,
        "reason_pr": pr_reason,
        "reason_at": at_reason,
        "reason_br": br_reason,
    }
    return response


async def _compute_pr_route(start_lat, start_lon, end_lat, end_lon,
                            nearest_pr, arrival_min, car_only, req) -> Optional[dict]:
    """Compute Park & Ride route using the best P+R station found."""
    max_walk = req.max_walk_m
    for pr in nearest_pr:
        # Segment 1: Drive to P+R
        car_seg = await osrm_route(start_lat, start_lon, pr["lat"], pr["lon"])

        # Segment 2: Walk from P+R to nearest transit stop
        entry_stop = _find_stop_near(pr["lat"], pr["lon"], max_m=max_walk)
        if not entry_stop:
            continue

        # Segment 4: Walk from exit stop to destination
        exit_stop = find_nearest_stop(end_lat, end_lon, max_km=max_walk / 1000)
        if not exit_stop:
            continue

        # Segment 3: ÖPNV routing
        # Calculate backwards: need to arrive at exit_stop before (arrival_min - walk_time_to_dest)
        walk_to_dest_min = exit_stop["distance_km"] / WALK_SPEED_KMH * 60
        latest_exit = int(arrival_min - walk_to_dest_min)

        # Estimate earliest possible departure from entry stop
        drive_time = car_seg["duration_min"]
        walk_to_stop_min = entry_stop["distance_km"] / WALK_SPEED_KMH * 60
        earliest_depart = int(arrival_min - 120)  # Search window: up to 2h before arrival
        if earliest_depart < 0:
            earliest_depart = 0

        chosen_algo = req.algorithm if req.algorithm in ("astar", "dijkstra", "greedy") else "astar"
        best_result = run_algorithm(
            transit_graph, entry_stop["stop_id"], exit_stop["stop_id"],
            earliest_depart, chosen_algo
        )
        algo_results = run_all_algorithms(
            transit_graph, entry_stop["stop_id"], exit_stop["stop_id"], earliest_depart
        )

        if not best_result or not best_result.found:
            continue

        # Calculate ÖPNV distance (sum of Haversine between consecutive stops)
        transit_km = 0
        for i in range(len(best_result.path) - 1):
            s1 = best_result.path[i]
            s2 = best_result.path[i + 1]
            c1 = transit_graph.stop_coords.get(s1["stop_id"])
            c2 = transit_graph.stop_coords.get(s2["stop_id"])
            if c1 and c2:
                transit_km += haversine_km(c1[0], c1[1], c2[0], c2[1])

        # Totals
        total_duration = (car_seg["duration_min"] + walk_to_stop_min +
                          best_result.total_duration_min + walk_to_dest_min)
        auto_km = car_seg["distance_km"]
        co2 = auto_km * CO2_CAR_G_PER_KM + transit_km * CO2_TRANSIT_G_PER_KM
        cost = auto_km * CAR_COST_EUR_PER_KM + TRANSIT_COST_PER_TRIP_EUR

        score = compute_score(
            co2_car=car_only["co2_g"], co2_alt=co2,
            time_car=car_only["duration_min"], time_alt=total_duration,
            cost_car=car_only["cost_eur"], cost_alt=cost,
            w_co2=req.weight_co2, w_time=req.weight_time, w_cost=req.weight_cost,
        )

        return {
            "segments": [
                {"type": "car", "from": "Start", "to": pr["name"],
                 "distance_km": auto_km, "duration_min": round(car_seg["duration_min"], 1),
                 "geometry": car_seg.get("geometry")},
                {"type": "walk", "from": pr["name"], "to": entry_stop["name"],
                 "distance_m": round(entry_stop["distance_km"] * 1000),
                 "duration_min": round(walk_to_stop_min, 1)},
                {"type": "transit", "from": entry_stop["name"], "to": exit_stop["name"],
                 "distance_km": round(transit_km, 2),
                 "duration_min": best_result.total_duration_min,
                 "path": best_result.path},
                {"type": "walk", "from": exit_stop["name"], "to": "Ziel",
                 "distance_m": round(exit_stop["distance_km"] * 1000),
                 "duration_min": round(walk_to_dest_min, 1)},
            ],
            "total_duration_min": round(total_duration, 1),
            "co2_g": round(co2),
            "cost_eur": round(cost, 2),
            "score": score,
            "parking_station": {"name": pr["name"], "lat": pr["lat"], "lon": pr["lon"],
                                "available": pr.get("available"),
                                "has_realtime": pr.get("has_realtime", False)},
            "algorithm_results": {k: v.to_dict() for k, v in algo_results.items()},
        }

    return None


async def _compute_auto_transit_route(start_lat, start_lon, end_lat, end_lon,
                                      arrival_min, car_only, req) -> Optional[dict]:
    """Compute Auto + ÖPNV route: drive to nearest transit stop, take transit.
    No dedicated P+R station needed — just park near a stop."""
    max_walk = req.max_walk_m

    # Find the 5 nearest stops to the start (within 15 km driving)
    candidates = []
    for sid, (slat, slon) in transit_graph.stop_coords.items():
        d = haversine_km(start_lat, start_lon, slat, slon)
        if d <= 15:
            candidates.append((sid, slat, slon, d))
    candidates.sort(key=lambda x: x[3])
    candidates = candidates[:8]

    if not candidates:
        return None

    # Exit stop near destination
    exit_stop = find_nearest_stop(end_lat, end_lon, max_km=max_walk / 1000)
    if not exit_stop:
        return None

    walk_to_dest_min = exit_stop["distance_km"] / WALK_SPEED_KMH * 60

    for sid, slat, slon, dist_km in candidates:
        # Drive to area near stop (or walk if very close)
        car_seg = await osrm_route(start_lat, start_lon, slat, slon)

        earliest_depart = int(arrival_min - 120)
        if earliest_depart < 0:
            earliest_depart = 0

        chosen_algo = req.algorithm if req.algorithm in ("astar", "dijkstra", "greedy") else "astar"
        best_result = run_algorithm(
            transit_graph, sid, exit_stop["stop_id"],
            earliest_depart, chosen_algo
        )
        algo_results = run_all_algorithms(
            transit_graph, sid, exit_stop["stop_id"], earliest_depart
        )

        if not best_result or not best_result.found:
            continue

        # Transit distance
        transit_km = 0
        for i in range(len(best_result.path) - 1):
            s1 = best_result.path[i]
            s2 = best_result.path[i + 1]
            c1 = transit_graph.stop_coords.get(s1["stop_id"])
            c2 = transit_graph.stop_coords.get(s2["stop_id"])
            if c1 and c2:
                transit_km += haversine_km(c1[0], c1[1], c2[0], c2[1])

        stop_name = transit_graph.stop_names.get(sid, "Haltestelle")
        auto_km = car_seg["distance_km"]
        total_duration = (car_seg["duration_min"] +
                          best_result.total_duration_min + walk_to_dest_min)
        co2 = auto_km * CO2_CAR_G_PER_KM + transit_km * CO2_TRANSIT_G_PER_KM
        cost = auto_km * CAR_COST_EUR_PER_KM + TRANSIT_COST_PER_TRIP_EUR

        score = compute_score(
            co2_car=car_only["co2_g"], co2_alt=co2,
            time_car=car_only["duration_min"], time_alt=total_duration,
            cost_car=car_only["cost_eur"], cost_alt=cost,
            w_co2=req.weight_co2, w_time=req.weight_time, w_cost=req.weight_cost,
        )

        return {
            "segments": [
                {"type": "car", "from": "Start", "to": stop_name,
                 "distance_km": auto_km, "duration_min": round(car_seg["duration_min"], 1),
                 "geometry": car_seg.get("geometry")},
                {"type": "transit", "from": stop_name, "to": exit_stop["name"],
                 "distance_km": round(transit_km, 2),
                 "duration_min": best_result.total_duration_min,
                 "path": best_result.path},
                {"type": "walk", "from": exit_stop["name"], "to": "Ziel",
                 "distance_m": round(exit_stop["distance_km"] * 1000),
                 "duration_min": round(walk_to_dest_min, 1)},
            ],
            "total_duration_min": round(total_duration, 1),
            "co2_g": round(co2),
            "cost_eur": round(cost, 2),
            "score": score,
            "entry_stop": {"name": stop_name, "lat": slat, "lon": slon},
            "algorithm_results": {k: v.to_dict() for k, v in algo_results.items()},
        }

    return None


async def _compute_br_route(start_lat, start_lon, end_lat, end_lon,
                            nearest_br, arrival_min, car_only, req) -> Optional[dict]:
    """Compute Bike & Ride route using the best B+R station found."""
    max_walk = req.max_walk_m
    for br in nearest_br:
        # Segment 1: Bike to B+R
        bike_dist_km = haversine_km(start_lat, start_lon, br["lat"], br["lon"]) * DETOUR_INDEX_BIKE
        bike_time_min = bike_dist_km / BIKE_SPEED_KMH * 60

        # Segment 2: Walk from B+R to nearest transit stop
        entry_stop = _find_stop_near(br["lat"], br["lon"], max_m=min(max_walk, 500))
        if not entry_stop:
            continue

        # Segment 4: Walk from exit stop to destination
        exit_stop = find_nearest_stop(end_lat, end_lon, max_km=max_walk / 1000)
        if not exit_stop:
            continue

        walk_to_dest_min = exit_stop["distance_km"] / WALK_SPEED_KMH * 60
        walk_to_stop_min = entry_stop["distance_km"] / WALK_SPEED_KMH * 60

        earliest_depart = int(arrival_min - 120)
        if earliest_depart < 0:
            earliest_depart = 0

        chosen_algo = req.algorithm if req.algorithm in ("astar", "dijkstra", "greedy") else "astar"
        best_result = run_algorithm(
            transit_graph, entry_stop["stop_id"], exit_stop["stop_id"],
            earliest_depart, chosen_algo
        )
        algo_results = run_all_algorithms(
            transit_graph, entry_stop["stop_id"], exit_stop["stop_id"], earliest_depart
        )

        if not best_result or not best_result.found:
            continue

        transit_km = 0
        for i in range(len(best_result.path) - 1):
            s1 = best_result.path[i]
            s2 = best_result.path[i + 1]
            c1 = transit_graph.stop_coords.get(s1["stop_id"])
            c2 = transit_graph.stop_coords.get(s2["stop_id"])
            if c1 and c2:
                transit_km += haversine_km(c1[0], c1[1], c2[0], c2[1])

        total_duration = (bike_time_min + walk_to_stop_min +
                          best_result.total_duration_min + walk_to_dest_min)
        co2 = transit_km * CO2_TRANSIT_G_PER_KM  # Bike segment = 0 CO2
        cost = BIKE_COST_PER_TRIP_EUR + TRANSIT_COST_PER_TRIP_EUR

        score = compute_score(
            co2_car=car_only["co2_g"], co2_alt=co2,
            time_car=car_only["duration_min"], time_alt=total_duration,
            cost_car=car_only["cost_eur"], cost_alt=cost,
            w_co2=req.weight_co2, w_time=req.weight_time, w_cost=req.weight_cost,
        )

        return {
            "segments": [
                {"type": "bike", "from": "Start", "to": br["name"],
                 "distance_km": round(bike_dist_km, 2),
                 "duration_min": round(bike_time_min, 1)},
                {"type": "walk", "from": br["name"], "to": entry_stop["name"],
                 "distance_m": round(entry_stop["distance_km"] * 1000),
                 "duration_min": round(walk_to_stop_min, 1)},
                {"type": "transit", "from": entry_stop["name"], "to": exit_stop["name"],
                 "distance_km": round(transit_km, 2),
                 "duration_min": best_result.total_duration_min,
                 "path": best_result.path},
                {"type": "walk", "from": exit_stop["name"], "to": "Ziel",
                 "distance_m": round(exit_stop["distance_km"] * 1000),
                 "duration_min": round(walk_to_dest_min, 1)},
            ],
            "total_duration_min": round(total_duration, 1),
            "co2_g": round(co2),
            "cost_eur": round(cost, 2),
            "score": score,
            "bike_station": {"name": br["name"], "lat": br["lat"], "lon": br["lon"],
                             "type": br.get("type", ""), "capacity": br.get("capacity", 0)},
            "algorithm_results": {k: v.to_dict() for k, v in algo_results.items()},
        }

    return None


def _find_stop_near(lat: float, lon: float, max_m: float) -> Optional[dict]:
    """Find nearest stop within max_m meters. Uses Neo4j data via NEAR_STOP or direct search."""
    if not transit_graph:
        return None
    best = None
    best_dist = max_m / 1000  # Convert to km
    for sid, (slat, slon) in transit_graph.stop_coords.items():
        d = haversine_km(lat, lon, slat, slon)
        if d < best_dist:
            best_dist = d
            best = {"stop_id": sid, "name": transit_graph.stop_names[sid],
                    "lat": slat, "lon": slon, "distance_km": round(d, 3)}
    return best


@app.get("/benchmark")
async def benchmark(n: int = Query(default=100, ge=10, le=1000)):
    """Run n random routing pairs through all three algorithms."""
    if not transit_graph or transit_graph.n_stops < 2:
        raise HTTPException(503, "Transit graph not loaded.")

    stop_ids = list(transit_graph.stop_coords.keys())
    departure_times = [420, 450, 480, 510, 540]  # 7:00 - 9:00 in 30-min steps

    results_by_algo: dict[str, list[dict]] = {"greedy": [], "dijkstra": [], "astar": []}

    for _ in range(n):
        s1, s2 = random.sample(stop_ids, 2)
        dep = random.choice(departure_times)
        algo_results = run_all_algorithms(transit_graph, s1, s2, dep)
        for name, result in algo_results.items():
            results_by_algo[name].append({
                "runtime_ms": result.runtime_ms,
                "nodes_expanded": result.nodes_expanded,
                "cost": result.cost if result.found else None,
                "found": result.found,
                "duration_min": result.total_duration_min if result.found else None,
            })

    # Aggregate
    summary = {}
    dijkstra_costs = {}
    for i, r in enumerate(results_by_algo["dijkstra"]):
        if r["found"]:
            dijkstra_costs[i] = r["cost"]

    for algo_name, runs in results_by_algo.items():
        runtimes = [r["runtime_ms"] for r in runs]
        nodes = [r["nodes_expanded"] for r in runs]
        costs = [r["cost"] for r in runs if r["found"]]
        found_count = sum(1 for r in runs if r["found"])

        runtimes_sorted = sorted(runtimes)
        p95_idx = int(len(runtimes_sorted) * 0.95)

        # Path optimality vs Dijkstra
        optimality_ratios = []
        for i, r in enumerate(runs):
            if r["found"] and i in dijkstra_costs and dijkstra_costs[i] > 0:
                optimality_ratios.append(r["cost"] / dijkstra_costs[i])

        summary[algo_name] = {
            "avg_runtime_ms": round(sum(runtimes) / len(runtimes), 2) if runtimes else 0,
            "median_runtime_ms": round(runtimes_sorted[len(runtimes_sorted) // 2], 2) if runtimes else 0,
            "p95_runtime_ms": round(runtimes_sorted[p95_idx], 2) if runtimes else 0,
            "avg_nodes_expanded": round(sum(nodes) / len(nodes), 1) if nodes else 0,
            "avg_path_cost": round(sum(costs) / len(costs), 2) if costs else 0,
            "path_optimality_vs_dijkstra": round(
                sum(optimality_ratios) / len(optimality_ratios), 4
            ) if optimality_ratios else None,
            "n_found": found_count,
            "n_no_path_found": len(runs) - found_count,
        }

    return {"n": n, "algorithms": summary}


@app.get("/score-sensitivity")
async def score_sensitivity(
    start_lat: float = Query(...), start_lon: float = Query(...),
    end_lat: float = Query(...), end_lon: float = Query(...),
    arrival_time: str = Query(default="08:30"),
):
    """Compute Pendler-Score with 10 different weight combinations."""
    # First compute the base route
    req = RouteRequest(start_lat=start_lat, start_lon=start_lon,
                       end_lat=end_lat, end_lon=end_lon,
                       arrival_time=arrival_time)
    route_data = await compute_route(req)

    weight_combos = [
        {"co2": 0.4, "time": 0.35, "cost": 0.25, "label": "Default"},
        {"co2": 0.6, "time": 0.2, "cost": 0.2, "label": "Umwelt-Fokus"},
        {"co2": 0.2, "time": 0.6, "cost": 0.2, "label": "Zeit-Fokus"},
        {"co2": 0.2, "time": 0.2, "cost": 0.6, "label": "Kosten-Fokus"},
        {"co2": 0.33, "time": 0.33, "cost": 0.34, "label": "Gleichgewicht"},
        {"co2": 0.8, "time": 0.1, "cost": 0.1, "label": "Stark Umwelt"},
        {"co2": 0.1, "time": 0.8, "cost": 0.1, "label": "Stark Zeit"},
        {"co2": 0.1, "time": 0.1, "cost": 0.8, "label": "Stark Kosten"},
        {"co2": 0.5, "time": 0.5, "cost": 0.0, "label": "Ohne Kosten"},
        {"co2": 0.0, "time": 0.5, "cost": 0.5, "label": "Ohne CO2"},
    ]

    car = route_data["car_only"]
    results = []

    for wc in weight_combos:
        entry = {"weights": wc}
        if route_data.get("park_and_ride"):
            pr = route_data["park_and_ride"]
            entry["pr_score"] = compute_score(
                car["co2_g"], pr["co2_g"],
                car["duration_min"], pr["total_duration_min"],
                car["cost_eur"], pr["cost_eur"],
                wc["co2"], wc["time"], wc["cost"],
            )
        else:
            entry["pr_score"] = None

        if route_data.get("bike_and_ride"):
            br = route_data["bike_and_ride"]
            entry["br_score"] = compute_score(
                car["co2_g"], br["co2_g"],
                car["duration_min"], br["total_duration_min"],
                car["cost_eur"], br["cost_eur"],
                wc["co2"], wc["time"], wc["cost"],
            )
        else:
            entry["br_score"] = None

        results.append(entry)

    return {"route_summary": {
        "car_duration_min": car["duration_min"],
        "car_co2_g": car["co2_g"],
    }, "sensitivity": results}


@app.get("/stops")
async def get_stops():
    """Return all transit stops for map display."""
    if not transit_graph:
        return []
    return [
        {"stop_id": sid, "name": transit_graph.stop_names[sid],
         "lat": coords[0], "lon": coords[1]}
        for sid, coords in transit_graph.stop_coords.items()
    ]


@app.get("/parking")
async def get_parking():
    """Return all P+R and B+R stations."""
    return {
        "park_ride": park_ride_stations,
        "bike_ride": bike_ride_stations,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
