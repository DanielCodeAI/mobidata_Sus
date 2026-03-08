import pandas as pd
import numpy as np
import networkx as nx
from haversine import haversine, Unit
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sys

# ==========================================
# 1. Data Processing and Graph Generation
# ==========================================

def load_gtfs_data(data_path='bwgesamt/'):
    print(f"Loading FULL GTFS data from {data_path}...")
    try:
        stops      = pd.read_csv(f'{data_path}stops.txt', dtype=str)
        stop_times = pd.read_csv(f'{data_path}stop_times.txt', dtype=str) 
        trips      = pd.read_csv(f'{data_path}trips.txt', dtype=str)
        routes     = pd.read_csv(f'{data_path}routes.txt', dtype=str)
        
        stops['stop_lat'] = pd.to_numeric(stops['stop_lat'])
        stops['stop_lon'] = pd.to_numeric(stops['stop_lon'])
        stop_times['stop_sequence'] = pd.to_numeric(stop_times['stop_sequence'])

        stops      = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
        stop_times = stop_times[['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
        routes     = routes[['route_id', 'route_short_name', 'route_type']]
        trips      = trips[['trip_id', 'route_id']]
            
        print("Data imported successfully.")
        return stops, stop_times, trips, routes
    except Exception as e:
        print(f"Error loading GTFS data: {e}")
        sys.exit(1)

def calc_distance_km(lat1, lon1, lat2, lon2): 
    return haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)

def create_transit_graph(stop_times, trips, stops, routes):
    print("Creating Transit Edges (This may take a minute for the full BW dataset)...")
    
    merged = stop_times.merge(trips, on='trip_id').merge(routes, on='route_id')
    merged = merged.sort_values(['trip_id', 'stop_sequence'])
    
    # We only care about topology for edge generation, deduplicate edges!
    # A lot of buses travel the same path. Group by (stop A -> stop B, route_id) to reduce edges massivey!
    merged['next_stop_id'] = merged.groupby('trip_id')['stop_id'].shift(-1)
    edges = merged.dropna(subset=['next_stop_id']).copy()
    
    # Deduplicate: we just need one edge per route between A and B
    edges = edges.drop_duplicates(subset=['stop_id', 'next_stop_id', 'route_short_name', 'route_type'])
    
    edges = edges.merge(stops[['stop_id', 'stop_lat', 'stop_lon', 'stop_name']], on='stop_id')
    edges = edges.merge(
        stops[['stop_id', 'stop_lat', 'stop_lon', 'stop_name']], 
        left_on='next_stop_id', 
        right_on='stop_id', 
        suffixes=('_start', '_end')
    )
    
    print("Calculating distances...")
    edges['distance_km'] = edges.apply(
        lambda x: calc_distance_km(x['stop_lat_start'], x['stop_lon_start'], 
                                   x['stop_lat_end'], x['stop_lon_end']), axis=1
    )
    
    edges['time_min'] = edges['distance_km'] * 2.5 # approx 2.5 mins per km
    
    print("Building NetworkX Graph...")
    G = nx.MultiDiGraph()
    for _, row in edges.iterrows():
        G.add_edge(
            row['stop_id_start'], 
            row['stop_id_end'], 
            trip_id=row['trip_id'],
            route_short_name=row['route_short_name'],
            route_type=row['route_type'],
            distance_km=row['distance_km'],
            time_min=row['time_min'],
            start_name=row['stop_name_start'],
            end_name=row['stop_name_end']
        )
        
    print(f"Graph built with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")
    return G, stops

# ==========================================
# 2. FastAPI Backend Setup
# ==========================================

app = FastAPI(title="Multimodal Routing API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

G = None
STOPS_DF = None

@app.on_event("startup")
def startup_event():
    global G, STOPS_DF
    stops, stop_times, trips, routes = load_gtfs_data()
    G, STOPS_DF = create_transit_graph(stop_times, trips, stops, routes)

class RouteRequest(BaseModel):
    start_stop_id: str
    end_stop_id: str
    time_vs_co2_weight: float = 0.5 
    algorithm: str = "dijkstra" 
    co2_config: dict = None

@app.get("/stops")
def get_stops():
    if STOPS_DF is None:
        return []
        
    valid_nodes = set(G.nodes())
    valid_stops = STOPS_DF[STOPS_DF['stop_id'].isin(valid_nodes)]
    
    # DEDUPLICATE STOPS BY NAME:
    unique_stops = valid_stops.drop_duplicates(subset=['stop_name'])
    
    return unique_stops[['stop_id', 'stop_name']].to_dict('records')

@app.post("/route")
def calculate_route(req: RouteRequest):
    if req.start_stop_id not in G or req.end_stop_id not in G:
        return {"error": "Start- oder Zielhaltestelle im Routing-Netzwerk nicht gefunden."}
        
    alpha = 1.0 - req.time_vs_co2_weight 
    beta = req.time_vs_co2_weight 
    
    # Parse dynamic CO2 mapping
    co2_map = {
        '0': 40, '1': 30, '2': 35, '3': 80  # Default fallback
    }
    if req.co2_config:
        co2_map = {
            '0': req.co2_config.get('tram', 40),
            '1': req.co2_config.get('subway', 30),
            '2': req.co2_config.get('rail', 35),
            '3': req.co2_config.get('bus', 80)
        }
    
    # Dynamic edge weight function
    def get_dynamic_co2(route_type, distance):
        g_per_km = co2_map.get(str(route_type), 50)
        return g_per_km * distance

    def edge_weight(u, v, d):
        # In a MultiDiGraph, d is a dictionary of dictionaries (keys are 0, 1, 2...)
        # We need to find the edge with the minimum cost between u and v
        min_cost = float('inf')
        for key, edge_data in d.items():
            co2_cost = get_dynamic_co2(edge_data['route_type'], edge_data['distance_km'])
            cost = alpha * edge_data['time_min'] + beta * (co2_cost / 10.0)
            if cost < min_cost:
                min_cost = cost
        return min_cost
        
    def astar_heuristic(u, v):
        # Haversine distance from current node `u` to target node `v`
        try:
            u_data = STOPS_DF[STOPS_DF['stop_id'] == u].iloc[0]
            v_data = STOPS_DF[STOPS_DF['stop_id'] == v].iloc[0]
            dist = calc_distance_km(u_data['stop_lat'], u_data['stop_lon'], v_data['stop_lat'], v_data['stop_lon'])
            
            # Estimate best-case scenario (e.g. fastest possible transport like a train with 40g CO2, and 1 min/km)
            # This is an admissible heuristic for the weighted cost function
            best_time = dist * 1.0  # Assumed 60km/h
            best_co2 = dist * 30.0  # Subway equivalent
            return alpha * best_time + beta * (best_co2 / 10.0)
        except:
            return 0
        
    try:
        if req.algorithm.lower() == 'greedy' or req.algorithm.lower() == 'astar':
            path = nx.astar_path(G, source=req.start_stop_id, target=req.end_stop_id, heuristic=astar_heuristic, weight=edge_weight)
        else:
            path = nx.shortest_path(G, source=req.start_stop_id, target=req.end_stop_id, weight=edge_weight)
        
        raw_steps = []
        total_time = 0.0
        total_dist = 0.0
        total_co2 = 0.0
        
        for i in range(len(path) - 1):
            u = path[i]
            v = path[i+1]
            
            # Since it's a MultiDiGraph, get the edge with minimum dynamic weight
            edge_data = G[u][v]
            
            # The edge_data is a dict like: {0: {'route_type': '3', ...}, 1: {'route_type': '2', ...}}
            # To find the best key, we have to evaluate our `edge_weight(u, v, internal_d)` 
            # Notice that `edge_weight` expects `d` to be the FULL dictionary of dictionaries `{0: {...}}`
            # This is because `nx.shortest_path` passes the full `d` to `weight`.
            # To find the best edge for our manual step extraction, we just iterate the same way:
            best_key = None
            min_cost = float('inf')
            for k, data in edge_data.items():
                cost = get_dynamic_co2(data['route_type'], data['distance_km']) * (beta / 10.0) + alpha * data['time_min']
                if cost < min_cost:
                    min_cost = cost
                    best_key = k
                    
            e = edge_data[best_key]
            
            co2_g = get_dynamic_co2(e['route_type'], e['distance_km'])
            
            raw_steps.append({
                "from": e['start_name'],
                "to": e['end_name'],
                "from_lat": STOPS_DF[STOPS_DF['stop_id'] == u].iloc[0]['stop_lat'],
                "from_lon": STOPS_DF[STOPS_DF['stop_id'] == u].iloc[0]['stop_lon'],
                "to_lat": STOPS_DF[STOPS_DF['stop_id'] == v].iloc[0]['stop_lat'],
                "to_lon": STOPS_DF[STOPS_DF['stop_id'] == v].iloc[0]['stop_lon'],
                "line": e['route_short_name'],
                "type": e['route_type'],
                "time": e['time_min'],
                "distance": e['distance_km'],
                "co2": co2_g
            })
            total_time += e['time_min']
            total_dist += e['distance_km']
            total_co2 += co2_g
            
        # Deduplication Logic: Group continuous segments of the same line
        grouped_steps = []
        path_coordinates = [] # List of [lat, lon] tuples for drawing a polyline
        
        if raw_steps:
            current_step = dict(raw_steps[0])
            path_coordinates.append([current_step['from_lat'], current_step['from_lon']])
            
            for i in range(1, len(raw_steps)):
                next_step = raw_steps[i]
                path_coordinates.append([next_step['from_lat'], next_step['from_lon']])
                
                if current_step['line'] == next_step['line'] and current_step['type'] == next_step['type']:
                    # Extend current step
                    current_step['to'] = next_step['to']
                    current_step['time'] += next_step['time']
                    current_step['distance'] += next_step['distance']
                    current_step['co2'] += next_step['co2']
                else:
                    # Round specific step data before saving
                    current_step['time'] = round(current_step['time'], 1)
                    current_step['distance'] = round(current_step['distance'], 2)
                    current_step['co2'] = round(current_step['co2'], 1)
                    grouped_steps.append(current_step)
                    current_step = dict(next_step)
            
            # Add the last step coordinates and data
            path_coordinates.append([current_step['to_lat'], current_step['to_lon']])
            current_step['time'] = round(current_step['time'], 1)
            current_step['distance'] = round(current_step['distance'], 2)
            current_step['co2'] = round(current_step['co2'], 1)
            grouped_steps.append(current_step)
            
        return {
            "summary": {
                "totalTime": round(total_time, 1),
                "totalDistance": round(total_dist, 2),
                "totalCo2": round(total_co2, 1),
                "transfers": max(0, len(grouped_steps) - 1)
            },
            "steps": grouped_steps,
            "coordinates": path_coordinates
        }
            
    except nx.NetworkXNoPath:
        return {"error": "Keine Route gefunden."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
