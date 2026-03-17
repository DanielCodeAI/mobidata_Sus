# 🚌 MobiData – Multimodales Routing für Baden-Württemberg

Ein Prototyp für multimodales Routing mit CO₂-Vergleich, basierend auf echten GTFS-Daten aus Baden-Württemberg.

![Vue](https://img.shields.io/badge/Frontend-Vue%203-42b883)
![FastAPI](https://img.shields.io/badge/Backend-FastAPI-009688)
![NetworkX](https://img.shields.io/badge/Graph-NetworkX-orange)
![Leaflet](https://img.shields.io/badge/Map-Leaflet%20%2F%20OSM-blue)

## ✨ Features

- **60.000+ Haltestellen** aus ganz BW (Stuttgart, Freiburg, Karlsruhe, …)
- **Algorithmen-Vergleich**: Dijkstra vs. A* (Greedy Search)
- **CO₂-Kalkulation** pro Verkehrsmittel (Tram, Bus, Zug, U-Bahn) – live konfigurierbar
- **Zeit vs. Umwelt Slider** – gewichtet die Routenwahl dynamisch
- **OpenStreetMap-Karte** mit eingezeichneter Route (Leaflet)
- **Intelligente Transfer-Gruppierung** – fasst aufeinanderfolgende Haltestellen derselben Linie zusammen

## 🏗️ Architektur

```
┌──────────────────┐        ┌──────────────────┐
│   Vue 3 Frontend │  API   │  FastAPI Backend  │
│   (Vite + Leaflet)│◄─────►│  (Python + NX)    │
└──────────────────┘        └──────────────────┘
                                    │
                            ┌───────┴────────┐
                            │  NetworkX Graph │
                            │  (GTFS BW Data) │
                            └────────────────┘
```

## 🚀 Setup

### Voraussetzungen

- **Python 3.9+**
- **Node.js 18+**
- GTFS-Datensatz `bwgesamt/` im Projektroot (nicht im Repo enthalten)

### 1. Backend starten

```bash
# Virtual Environment erstellen & aktivieren
python3 -m venv venv
source venv/bin/activate

# Dependencies installieren
pip install fastapi uvicorn pandas networkx haversine python-dotenv numpy

# Server starten (lädt den Graph beim Start – ca. 1-2 Min.)
python3 main.py
```

Der Server läuft dann auf `http://localhost:8000`.

### 2. Frontend starten

```bash
cd frontend

# Dependencies installieren
npm install

# Dev-Server starten
npm run dev
```

Das Frontend läuft dann auf `http://localhost:5173`.

## 📁 Projektstruktur

```
├── main.py                 # FastAPI Backend + NetworkX Routing
├── ingest_neo4j.py         # (Optional) Daten nach Neo4j laden
├── .env                    # Neo4j Credentials (nicht im Repo)
├── bwgesamt/               # GTFS-Daten BW (nicht im Repo)
│   ├── stops.txt
│   ├── stop_times.txt
│   ├── trips.txt
│   └── routes.txt
└── frontend/
    ├── src/
    │   └── App.vue         # Haupt-UI (Preferences, Routing, Map, CO₂-Config)
    ├── package.json
    └── vite.config.js
```

## 🔌 API Endpoints

| Methode | Endpoint  | Beschreibung                        |
|---------|-----------|-------------------------------------|
| GET     | `/stops`  | Alle verfügbaren Haltestellen       |
| POST    | `/route`  | Route berechnen (Dijkstra oder A*)  |

### POST `/route` – Body Beispiel

```json
{
  "start_stop_id": "de:08111:110:0:1",
  "end_stop_id": "de:08111:6113:1:1",
  "time_vs_co2_weight": 0.5,
  "algorithm": "dijkstra",
  "co2_config": { "tram": 40, "subway": 30, "rail": 35, "bus": 80 }
}
```

## 🧪 Algorithmen

- **Dijkstra**: Findet garantiert den optimalen Pfad basierend auf der gewählten Gewichtung
- **A\* (Greedy Search)**: Nutzt eine Haversine-Heuristik zur Beschleunigung – tendenziell schneller, aber nicht immer optimal

## 📝 Lizenz

Prototyp für akademische Zwecke.
