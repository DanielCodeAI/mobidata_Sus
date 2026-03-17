#import all necessary libraries
import pandas as pd
import numpy as np
from haversine import haversine, Unit

def load_gtfs_data(data_path='data/'):
    #read all necessary data 
    stops      = pd.read_csv(f'{data_path}stops.txt')
    stop_times = pd.read_csv(f'{data_path}stop_times.txt')
    trips      = pd.read_csv(f'{data_path}trips.txt')
    routes     = pd.read_csv(f'{data_path}routes.txt')
    #select only necessary columns
    stops      = stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']]
    stop_times = stop_times[['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence']]
    routes     = routes[['route_id', 'route_short_name', 'route_type']]
    trips      = trips[['trip_id', 'route_id', 'service_id']]

    print("data imported successfully")
    print(stops.head())
    print(stop_times.head())
    print(trips.head())
    print(routes.head())
    return stops, stop_times, trips, routes

#calculation of Distance between two coordinates in KM (with haversine formula)
def calc_distance(lat1, lon1, lat2, lon2): 
    loc1= (lat1, lon1)
    loc2= (lat2, lon2)  
    return haversine(loc1, loc2, unit=Unit.KILOMETERS)


# 3. KERNELEMENT: Erstellung der Verbindungen (Edges) für den Graphen
def create_transit_edges(stop_times, trips, stops, routes):
    print("Erstelle Verbindungen (Edges)...")
    
    # a) Wir brauchen Informationen zur Route (Bus/Bahn) an den Stop-Times
    # Merge: stop_times -> trips -> routes
    merged = stop_times.merge(trips, on='trip_id').merge(routes, on='route_id')
    
    # b) Sortieren, damit die Reihenfolge der Haltestellen stimmt
    merged = merged.sort_values(['trip_id', 'stop_sequence'])
    
    # c) "Shift", um die nächste Haltestelle in die gleiche Zeile zu bekommen
    # Das erzeugt eine Verbindung: Von Stop A -> Nach Stop B
    merged['next_stop_id'] = merged.groupby('trip_id')['stop_id'].shift(-1)
    merged['next_stop_arr_time'] = merged.groupby('trip_id')['arrival_time'].shift(-1)
    
    # Letzte Haltestelle einer Fahrt hat keinen Nachfolger -> droppen
    edges = merged.dropna(subset=['next_stop_id'])
    
    # d) Koordinaten dazu holen (für Distanzberechnung)
    # Start-Koordinaten
    edges = edges.merge(stops[['stop_id', 'stop_lat', 'stop_lon']], on='stop_id')
    # Ziel-Koordinaten (join auf next_stop_id)
    edges = edges.merge(
        stops[['stop_id', 'stop_lat', 'stop_lon']], 
        left_on='next_stop_id', 
        right_on='stop_id', 
        suffixes=('_start', '_end')
    )
    
    # e) Distanz für jedes Segment berechnen
    # (Hier nutzen wir apply, für große Datenmengen ist Vektorisierung besser, aber so ist es lesbar)
    edges['distance_km'] = edges.apply(
        lambda x: calc_distance_km(x['stop_lat_start'], x['stop_lon_start'], 
                                   x['stop_lat_end'], x['stop_lon_end']), axis=1
    )
    
    # Rückgabe: Eine Tabelle, die jede Fahrt von A nach B beschreibt
    # Wichtig für Person B: Hier ist die 'route_type' (Bus/Bahn) und 'distance_km' drin!
    return edges
if __name__ == "__main__":
    stops, stop_times, trips, routes = load_gtfs_data()
    # Example usage of calc_distance function
    
    #Test coordinates (latitude and longitude)
    lat1, lon1 = 47.99779, 7.84261 # Freiburg HBF
    lat2, lon2 = 48.77585, 9.18293 # Stuttgart HBF

    distance = calc_distance(lat1, lon1, lat2, lon2)
    print(distance)



