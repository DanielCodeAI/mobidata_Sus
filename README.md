# MobiData Pendler-Routing — Baden-Württemberg

Multimodales Routing-Tool: Auto vs. Park&Ride vs. Bike&Ride.
Vergleicht drei Routing-Algorithmen (Greedy, Dijkstra, A*) auf GTFS-Daten.

## Setup (5 Befehle)

```bash
# 1. Neo4j starten
docker-compose up -d

# 2. Python-Abhängigkeiten installieren
pip install -r requirements.txt

# 3. P+R und B+R Stationen laden (ParkAPI v3, mit Fallback)
python fetch_parking.py

# 4. GTFS-Daten importieren (~300 MB Download, dauert einige Minuten)
python import_gtfs.py

# 5. Backend starten
python app.py
```

Frontend öffnen: http://localhost:8000

## CLI-Tools

```bash
# Jährlicher Sparreport
python savings_report.py --start "Lörrach" --end "Freiburg" --arrival 08:30

# Algorithmen-Benchmark (n zufällige Routenpaare)
python benchmark_report.py --n 200 --output results/
```

## Architektur

| Datei | Funktion |
|---|---|
| `docker-compose.yml` | Neo4j Community Edition |
| `fetch_parking.py` | P+R/B+R von MobiData BW ParkAPI v3 |
| `import_gtfs.py` | GTFS → Neo4j Graph (Stops, Connections, Transfers, P+R/B+R Links) |
| `routing.py` | Drei Algorithmen: GreedyRouter, DijkstraRouter, AStarRouter |
| `app.py` | FastAPI Backend (POST /route, GET /benchmark, GET /score-sensitivity) |
| `index.html` | Leaflet-Karte + Sidebar (Single-Page, kein Build-Step) |
| `savings_report.py` | CLI: Jährliche CO2/Kosten/Zeit-Ersparnis |
| `benchmark_report.py` | CLI: Algorithmenvergleich → JSON + CSV |

## Datenquellen

- GTFS BW: https://www.mobidata-bw.de/dataset/gtfs
- ParkAPI v3: https://api.mobidata-bw.de/park-api/api/public/v3/parking-sites
- Auto-Routing: OSRM (router.project-osrm.org)
- Geocoding: Nominatim (OSM)
- CO2-Faktoren: UBA 2024
