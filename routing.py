"""
routing.py — Drei ÖPNV-Routing-Algorithmen auf dem Neo4j-Zeitgraphen.

Alle drei implementieren dasselbe Interface:
    route(start_stop_id, end_stop_id, departure_time_min) → RoutingResult

Algorithmen:
1. GreedyRouter   — Immer nächste Abfahrt nehmen, keine Vorausschau
2. DijkstraRouter — Kürzeste Fahrzeit via klassischem Dijkstra
3. AStarRouter    — A* mit Haversine-Heuristik (admissible) [DEFAULT]

Verbesserungen:
- Wartezeit-Penalty: Wartezeiten >10 Min werden mit Faktor 0.5 bestraft
- Plattform-Zusammenführung: Bahnsteige gleicher Station = kostenloser Transfer
- Route-Type Labels: Tram/Bus/Regionalbahn im Pfad-Output
"""

import heapq
import logging
import math
import os
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from neo4j import GraphDatabase

log = logging.getLogger(__name__)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASS = os.environ.get("NEO4J_PASS", "mobidata2024")

TRANSFER_PENALTY_MIN = 3      # Pauschal 3 Min pro Umstieg
WAIT_PENALTY_THRESHOLD = 10   # Wartezeit ab der Penalty greift (Minuten)
WAIT_PENALTY_FACTOR = 0.5     # Zusätzliche Kosten pro Minute Wartezeit > Threshold

# GTFS route_type → Label
ROUTE_TYPE_LABELS = {
    0: "Tram",
    1: "U-Bahn",
    2: "Zug",
    3: "Bus",
    4: "Fähre",
    5: "Seilbahn",
    6: "Gondel",
    7: "Standseilbahn",
    100: "Regionalzug",
    101: "Fernzug",
    102: "Fernzug",
    103: "S-Bahn",
    109: "S-Bahn",
    400: "U-Bahn",
    700: "Bus",
    900: "Tram",
}


@dataclass
class RoutingResult:
    """Result of a single routing computation."""
    path: list[dict]            # List of {stop_id, stop_name, arrival, departure, route_name, route_type_label}
    total_duration_min: float
    num_transfers: int
    cost: float                 # Weighted cost used by the algorithm
    runtime_ms: float
    nodes_expanded: int
    algorithm: str
    found: bool = True

    def to_dict(self) -> dict:
        cost = self.cost if self.cost != float("inf") else None
        return {
            "path": self.path,
            "total_duration_min": round(self.total_duration_min, 1),
            "num_transfers": self.num_transfers,
            "cost": round(cost, 2) if cost is not None else None,
            "runtime_ms": round(self.runtime_ms, 2),
            "nodes_expanded": self.nodes_expanded,
            "algorithm": self.algorithm,
            "found": self.found,
        }


NO_RESULT = lambda algo: RoutingResult(
    path=[], total_duration_min=0, num_transfers=0,
    cost=float("inf"), runtime_ms=0, nodes_expanded=0,
    algorithm=algo, found=False,
)


@dataclass
class Connection:
    """A single scheduled connection between two stops."""
    from_id: str
    to_id: str
    departure: int      # Minutes since midnight
    arrival: int        # Minutes since midnight
    duration: int       # Minutes
    trip_id: str
    route_id: str
    route_name: str
    route_type: int = 3  # GTFS route_type (default: Bus)


@dataclass
class Transfer:
    """A walking transfer between two nearby stops."""
    from_id: str
    to_id: str
    walk_min: float


class TransitGraph:
    """In-memory representation of the transit network loaded from Neo4j."""

    def __init__(self):
        self.connections_from: dict[str, list[Connection]] = {}  # stop_id → outgoing connections
        self.transfers_from: dict[str, list[Transfer]] = {}      # stop_id → outgoing transfers
        self.stop_coords: dict[str, tuple[float, float]] = {}   # stop_id → (lat, lon)
        self.stop_names: dict[str, str] = {}                     # stop_id → name
        self.platform_groups: dict[str, str] = {}                # stop_id → canonical_id (platform merging)

    def load_from_neo4j(self) -> None:
        """Load the entire transit graph into memory."""
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
        try:
            with driver.session() as s:
                # Load stops
                result = s.run(
                    "MATCH (s:Stop) RETURN s.stop_id AS id, s.name AS name, s.lat AS lat, s.lon AS lon"
                )
                for r in result:
                    self.stop_coords[r["id"]] = (r["lat"], r["lon"])
                    self.stop_names[r["id"]] = r["name"]

                # Load connections (NEXT_STOP relationships)
                result = s.run("""
                    MATCH (a:Stop)-[r:NEXT_STOP]->(b:Stop)
                    RETURN a.stop_id AS from_id, b.stop_id AS to_id,
                           r.departure AS dep, r.arrival AS arr,
                           r.duration_min AS dur, r.trip_id AS trip,
                           r.route_id AS route, r.route_name AS rname,
                           r.route_type AS rtype
                """)
                for r in result:
                    conn = Connection(
                        from_id=r["from_id"], to_id=r["to_id"],
                        departure=r["dep"], arrival=r["arr"],
                        duration=r["dur"], trip_id=r["trip"],
                        route_id=r["route"], route_name=r["rname"],
                        route_type=r["rtype"] if r.get("rtype") is not None else 3,
                    )
                    self.connections_from.setdefault(conn.from_id, []).append(conn)

                # Sort connections by departure time for efficient lookup
                for sid in self.connections_from:
                    self.connections_from[sid].sort(key=lambda c: c.departure)

                # Load transfers
                result = s.run("""
                    MATCH (a:Stop)-[r:TRANSFER]->(b:Stop)
                    RETURN a.stop_id AS from_id, b.stop_id AS to_id, r.walk_min AS walk
                """)
                for r in result:
                    tr = Transfer(from_id=r["from_id"], to_id=r["to_id"], walk_min=r["walk"])
                    self.transfers_from.setdefault(tr.from_id, []).append(tr)

            # Build platform groups AFTER loading all data
            self._build_platform_groups()

        finally:
            driver.close()

    def _build_platform_groups(self) -> None:
        """Group stops that are platforms of the same physical station.
        E.g. 'Freiburg Hauptbahnhof Bstg 1' and 'Freiburg Hauptbahnhof Bstg 2'
        get linked with 0-cost transfers."""
        # Group by base name (strip platform suffixes)
        name_groups: dict[str, list[str]] = defaultdict(list)
        for sid, name in self.stop_names.items():
            # Strip common platform suffixes
            base = name
            for suffix in [" Bstg ", " Gleis ", " Steig ", " Haltest."]:
                if suffix in base:
                    base = base[:base.index(suffix)]
                    break
            name_groups[base].append(sid)

        # Create free transfers between platforms of same station
        platform_transfers_added = 0
        for base_name, stop_ids in name_groups.items():
            if len(stop_ids) < 2:
                continue
            # Only group if they're physically close (< 200m)
            for i, sid1 in enumerate(stop_ids):
                c1 = self.stop_coords.get(sid1)
                if not c1:
                    continue
                for sid2 in stop_ids[i + 1:]:
                    c2 = self.stop_coords.get(sid2)
                    if not c2:
                        continue
                    dist = _haversine_km(c1[0], c1[1], c2[0], c2[1]) * 1000
                    if dist < 200:
                        # Add free transfer (1 min walk within station)
                        existing_from = {t.to_id for t in self.transfers_from.get(sid1, [])}
                        if sid2 not in existing_from:
                            self.transfers_from.setdefault(sid1, []).append(
                                Transfer(from_id=sid1, to_id=sid2, walk_min=1.0)
                            )
                            self.transfers_from.setdefault(sid2, []).append(
                                Transfer(from_id=sid2, to_id=sid1, walk_min=1.0)
                            )
                            platform_transfers_added += 2

        if platform_transfers_added:
            log.info("Added %d platform transfers between %d station groups.",
                     platform_transfers_added,
                     sum(1 for g in name_groups.values() if len(g) > 1))

    @property
    def n_stops(self) -> int:
        return len(self.stop_coords)

    @property
    def n_connections(self) -> int:
        return sum(len(v) for v in self.connections_from.values())


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _route_type_label(route_type: int) -> str:
    """Convert GTFS route_type to human-readable German label."""
    return ROUTE_TYPE_LABELS.get(route_type, "Bus")


def _wait_cost(wait_minutes: float) -> float:
    """Calculate cost penalty for waiting.
    First 10 minutes are 'free' (normal takt), after that 0.5 min penalty per min."""
    if wait_minutes <= WAIT_PENALTY_THRESHOLD:
        return wait_minutes
    return WAIT_PENALTY_THRESHOLD + (wait_minutes - WAIT_PENALTY_THRESHOLD) * (1 + WAIT_PENALTY_FACTOR)


# ─── Base Router ──────────────────────────────────────────────────────────────

class BaseRouter(ABC):
    """Abstract base for all routing algorithms."""

    def __init__(self, graph: TransitGraph):
        self.graph = graph

    @abstractmethod
    def route(self, start_stop_id: str, end_stop_id: str,
              departure_time_min: int) -> RoutingResult:
        ...

    def _build_path(self, came_from: dict, end_id: str,
                    arrival_times: dict, departure_times: dict,
                    route_names: dict, route_types: dict = None) -> list[dict]:
        """Reconstruct path from came_from map."""
        if route_types is None:
            route_types = {}
        path = []
        current = end_id
        while current is not None:
            rt = route_types.get(current, 3)
            rn = route_names.get(current, "")
            path.append({
                "stop_id": current,
                "stop_name": self.graph.stop_names.get(current, ""),
                "arrival": arrival_times.get(current),
                "departure": departure_times.get(current),
                "route_name": rn,
                "route_type_label": "Fussweg" if rn == "walk" else _route_type_label(rt),
            })
            current = came_from.get(current)
        path.reverse()
        return path

    def _count_transfers(self, path: list[dict]) -> int:
        """Count transfers = number of route_name changes (excluding walking)."""
        if len(path) < 2:
            return 0
        transfers = 0
        prev_route = None
        for step in path:
            rn = step.get("route_name", "")
            if rn and rn != "walk" and prev_route and rn != prev_route:
                transfers += 1
            if rn and rn != "walk":
                prev_route = rn
        return transfers

    def _get_next_departures(self, stop_id: str, earliest: int,
                              max_wait: int = 60) -> list[Connection]:
        """Get all connections departing from stop_id at or after earliest,
        within max_wait minutes."""
        conns = self.graph.connections_from.get(stop_id, [])
        result = []
        for c in conns:
            if c.departure >= earliest and c.departure <= earliest + max_wait:
                result.append(c)
            elif c.departure > earliest + max_wait:
                break  # Sorted, so we can stop
        return result


# ─── Greedy Router ────────────────────────────────────────────────────────────

class GreedyRouter(BaseRouter):
    """
    Greedy nearest-departure algorithm.
    Strategy: At each stop, take the FIRST departing connection.
    No backtracking, no optimization. Pure greedy.
    This serves as the LOWER BASELINE — how bad does routing get
    without any optimization?
    """

    def route(self, start_stop_id: str, end_stop_id: str,
              departure_time_min: int) -> RoutingResult:
        t0 = time.perf_counter()
        nodes_expanded = 0

        if start_stop_id not in self.graph.stop_coords or end_stop_id not in self.graph.stop_coords:
            return NO_RESULT("greedy")

        visited = set()
        current = start_stop_id
        current_time = departure_time_min
        came_from: dict[str, Optional[str]] = {start_stop_id: None}
        arrival_times = {start_stop_id: departure_time_min}
        departure_times: dict[str, int] = {}
        route_names: dict[str, str] = {}
        route_types: dict[str, int] = {}

        max_hops = 50  # Safety limit to prevent infinite loops

        for _ in range(max_hops):
            if current == end_stop_id:
                path = self._build_path(came_from, end_stop_id,
                                        arrival_times, departure_times, route_names, route_types)
                duration = current_time - departure_time_min
                runtime = (time.perf_counter() - t0) * 1000
                return RoutingResult(
                    path=path, total_duration_min=duration,
                    num_transfers=self._count_transfers(path),
                    cost=duration, runtime_ms=runtime,
                    nodes_expanded=nodes_expanded, algorithm="greedy",
                )

            visited.add(current)
            nodes_expanded += 1

            # Try direct connections first
            best_conn = None
            departures = self._get_next_departures(current, current_time)
            for conn in departures:
                if conn.to_id not in visited:
                    best_conn = conn
                    break  # Greedy: take the first one

            # If no direct connection, try transfers
            if best_conn is None:
                for tr in self.graph.transfers_from.get(current, []):
                    if tr.to_id not in visited:
                        transfer_arrival = current_time + tr.walk_min
                        transfer_deps = self._get_next_departures(tr.to_id, transfer_arrival)
                        if transfer_deps:
                            came_from[tr.to_id] = current
                            arrival_times[tr.to_id] = transfer_arrival
                            route_names[tr.to_id] = "walk"
                            current = tr.to_id
                            current_time = transfer_arrival
                            best_conn = None
                            break

                if current in visited and best_conn is None:
                    break
                if best_conn is None:
                    continue

            if best_conn is not None:
                came_from[best_conn.to_id] = current
                departure_times[current] = best_conn.departure
                arrival_times[best_conn.to_id] = best_conn.arrival
                route_names[best_conn.to_id] = best_conn.route_name
                route_types[best_conn.to_id] = best_conn.route_type
                current_time = best_conn.arrival
                current = best_conn.to_id

        runtime = (time.perf_counter() - t0) * 1000
        result = NO_RESULT("greedy")
        result.runtime_ms = runtime
        result.nodes_expanded = nodes_expanded
        return result


# ─── Dijkstra Router ──────────────────────────────────────────────────────────

class DijkstraRouter(BaseRouter):
    """
    Dijkstra on the time-expanded transit graph with wait-time penalties.
    Edge weight = travel time + penalized wait time.
    This is the REFERENCE implementation — A* is measured against it.
    """

    def route(self, start_stop_id: str, end_stop_id: str,
              departure_time_min: int) -> RoutingResult:
        t0 = time.perf_counter()
        nodes_expanded = 0

        if start_stop_id not in self.graph.stop_coords or end_stop_id not in self.graph.stop_coords:
            return NO_RESULT("dijkstra")

        # Priority queue: (cost, arrival_time, stop_id)
        pq: list[tuple[float, int, str]] = [(0, departure_time_min, start_stop_id)]
        best_cost: dict[str, float] = {start_stop_id: 0}
        best_arrival: dict[str, int] = {start_stop_id: departure_time_min}
        came_from: dict[str, Optional[str]] = {start_stop_id: None}
        departure_times: dict[str, int] = {}
        route_names: dict[str, str] = {}
        route_types: dict[str, int] = {}

        while pq:
            current_cost, current_time, current = heapq.heappop(pq)

            if current_cost > best_cost.get(current, float("inf")):
                continue

            nodes_expanded += 1

            if current == end_stop_id:
                path = self._build_path(came_from, end_stop_id,
                                        best_arrival, departure_times, route_names, route_types)
                duration = current_time - departure_time_min
                runtime = (time.perf_counter() - t0) * 1000
                return RoutingResult(
                    path=path, total_duration_min=duration,
                    num_transfers=self._count_transfers(path),
                    cost=current_cost, runtime_ms=runtime,
                    nodes_expanded=nodes_expanded, algorithm="dijkstra",
                )

            for conn in self._get_next_departures(current, current_time):
                wait = conn.departure - current_time
                edge_cost = _wait_cost(wait) + conn.duration
                new_cost = current_cost + edge_cost
                if new_cost < best_cost.get(conn.to_id, float("inf")):
                    best_cost[conn.to_id] = new_cost
                    best_arrival[conn.to_id] = conn.arrival
                    came_from[conn.to_id] = current
                    departure_times[current] = conn.departure
                    route_names[conn.to_id] = conn.route_name
                    route_types[conn.to_id] = conn.route_type
                    heapq.heappush(pq, (new_cost, conn.arrival, conn.to_id))

            for tr in self.graph.transfers_from.get(current, []):
                edge_cost = tr.walk_min + TRANSFER_PENALTY_MIN
                new_cost = current_cost + edge_cost
                walk_arr = current_time + tr.walk_min + TRANSFER_PENALTY_MIN
                if new_cost < best_cost.get(tr.to_id, float("inf")):
                    best_cost[tr.to_id] = new_cost
                    best_arrival[tr.to_id] = walk_arr
                    came_from[tr.to_id] = current
                    route_names[tr.to_id] = "walk"
                    heapq.heappush(pq, (new_cost, walk_arr, tr.to_id))

        runtime = (time.perf_counter() - t0) * 1000
        result = NO_RESULT("dijkstra")
        result.runtime_ms = runtime
        result.nodes_expanded = nodes_expanded
        return result


# ─── A* Router ────────────────────────────────────────────────────────────────

class AStarRouter(BaseRouter):
    """
    A* with Haversine heuristic and wait-time penalties [DEFAULT ALGORITHM].
    Heuristic: straight-line distance to destination / max transit speed.
    Admissible because no vehicle exceeds MAX_SPEED_KMH.
    Expands fewer nodes than Dijkstra while finding optimal (or near-optimal) paths.
    """

    MAX_SPEED_KMH = 160.0  # ICE top speed — ensures heuristic is admissible

    def _heuristic(self, stop_id: str, end_stop_id: str) -> float:
        if stop_id == end_stop_id:
            return 0.0
        c1 = self.graph.stop_coords.get(stop_id)
        c2 = self.graph.stop_coords.get(end_stop_id)
        if not c1 or not c2:
            return 0.0
        dist_km = _haversine_km(c1[0], c1[1], c2[0], c2[1])
        return (dist_km / self.MAX_SPEED_KMH) * 60

    def route(self, start_stop_id: str, end_stop_id: str,
              departure_time_min: int) -> RoutingResult:
        t0 = time.perf_counter()
        nodes_expanded = 0

        if start_stop_id not in self.graph.stop_coords or end_stop_id not in self.graph.stop_coords:
            return NO_RESULT("astar")

        h0 = self._heuristic(start_stop_id, end_stop_id)
        # Priority queue: (f_score, arrival_time, stop_id)
        pq: list[tuple[float, int, str]] = [(h0, departure_time_min, start_stop_id)]
        g_score: dict[str, float] = {start_stop_id: 0}
        arrival_at: dict[str, int] = {start_stop_id: departure_time_min}
        came_from: dict[str, Optional[str]] = {start_stop_id: None}
        departure_times: dict[str, int] = {}
        route_names: dict[str, str] = {}
        route_types: dict[str, int] = {}

        while pq:
            f, current_time, current = heapq.heappop(pq)
            current_g = g_score.get(current, float("inf"))

            # Skip stale entries
            if f - self._heuristic(current, end_stop_id) > current_g + 0.01:
                continue

            nodes_expanded += 1

            if current == end_stop_id:
                path = self._build_path(came_from, end_stop_id,
                                        arrival_at, departure_times, route_names, route_types)
                duration = current_time - departure_time_min
                runtime = (time.perf_counter() - t0) * 1000
                return RoutingResult(
                    path=path, total_duration_min=duration,
                    num_transfers=self._count_transfers(path),
                    cost=current_g, runtime_ms=runtime,
                    nodes_expanded=nodes_expanded, algorithm="astar",
                )

            for conn in self._get_next_departures(current, current_time):
                wait = conn.departure - current_time
                edge_cost = _wait_cost(wait) + conn.duration
                new_g = current_g + edge_cost
                if new_g < g_score.get(conn.to_id, float("inf")):
                    g_score[conn.to_id] = new_g
                    arrival_at[conn.to_id] = conn.arrival
                    came_from[conn.to_id] = current
                    departure_times[current] = conn.departure
                    route_names[conn.to_id] = conn.route_name
                    route_types[conn.to_id] = conn.route_type
                    h = self._heuristic(conn.to_id, end_stop_id)
                    heapq.heappush(pq, (new_g + h, conn.arrival, conn.to_id))

            for tr in self.graph.transfers_from.get(current, []):
                edge_cost = tr.walk_min + TRANSFER_PENALTY_MIN
                new_g = current_g + edge_cost
                walk_arr = current_time + tr.walk_min + TRANSFER_PENALTY_MIN
                if new_g < g_score.get(tr.to_id, float("inf")):
                    g_score[tr.to_id] = new_g
                    arrival_at[tr.to_id] = walk_arr
                    came_from[tr.to_id] = current
                    route_names[tr.to_id] = "walk"
                    h = self._heuristic(tr.to_id, end_stop_id)
                    heapq.heappush(pq, (new_g + h, walk_arr, tr.to_id))

        runtime = (time.perf_counter() - t0) * 1000
        result = NO_RESULT("astar")
        result.runtime_ms = runtime
        result.nodes_expanded = nodes_expanded
        return result


# ─── Convenience functions ───────────────────────────────────────────────────

ROUTERS = {
    "greedy": GreedyRouter,
    "dijkstra": DijkstraRouter,
    "astar": AStarRouter,
}


def run_algorithm(graph: TransitGraph, start_id: str, end_id: str,
                  departure_min: int, algorithm: str = "astar") -> RoutingResult:
    """Run a single routing algorithm. Default: A*."""
    router_cls = ROUTERS.get(algorithm, AStarRouter)
    router = router_cls(graph)
    return router.route(start_id, end_id, departure_min)


def run_all_algorithms(graph: TransitGraph, start_id: str, end_id: str,
                       departure_min: int) -> dict[str, RoutingResult]:
    """Run all three algorithms and return results keyed by algorithm name."""
    results = {}
    for name, router_cls in ROUTERS.items():
        router = router_cls(graph)
        results[name] = router.route(start_id, end_id, departure_min)
    return results
