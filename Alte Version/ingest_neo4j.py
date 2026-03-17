import os
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load GTFS logic
from main import load_gtfs_data, create_transit_graph

# Load environment variables
load_dotenv()
URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

def ingest_to_neo4j():
    print(f"Connecting to Neo4j at {URI}...")
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    
    # 1. Load Data
    stops, stop_times, trips, routes = load_gtfs_data()
    G, STOPS_DF = create_transit_graph(stop_times, trips, stops, routes)
    
    with driver.session() as session:
        # Clear existing data (optional, but good for clean prototype)
        print("Clearing existing Neo4j Database...")
        session.run("MATCH (n) DETACH DELETE n")
        
        # 2. Upload Nodes (Stops)
        print(f"Uploading {len(STOPS_DF)} Nodes (Stops)...")
        # Ensure we only upload nodes that are part of the graph
        valid_stops = STOPS_DF[STOPS_DF['stop_id'].isin(G.nodes())]
        
        stops_data = valid_stops.to_dict('records')
        session.run('''
            UNWIND $stops AS stop
            CREATE (s:Stop {
                id: stop.stop_id,
                name: stop.stop_name,
                lat: stop.stop_lat,
                lon: stop.stop_lon
            })
        ''', stops=stops_data)
        
        # Create an index for faster lookups
        session.run("CREATE INDEX stop_id_index IF NOT EXISTS FOR (s:Stop) ON (s.id)")
        
        # 3. Upload Edges
        print(f"Uploading {G.number_of_edges()} Edges (Routes)...")
        edges_data = []
        for u, v, k, data in G.edges(keys=True, data=True):
            edges_data.append({
                'start_id': u,
                'end_id': v,
                'trip_id': data['trip_id'],
                'line': data['route_short_name'],
                'type': data['route_type'],
                'distance': data['distance_km'],
                'co2': data['co2_g'],
                'time': data['time_min']
            })
            
        # Batch insert edges
        batch_size = 5000
        for i in range(0, len(edges_data), batch_size):
            batch = edges_data[i:i + batch_size]
            print(f"Uploading edge batch {i} to {i+len(batch)}...")
            session.run('''
                UNWIND $edges AS edge
                MATCH (start:Stop {id: edge.start_id})
                MATCH (end:Stop {id: edge.end_id})
                CREATE (start)-[r:ROUTES_TO {
                    trip_id: edge.trip_id,
                    line: edge.line,
                    type: edge.type,
                    distance: edge.distance,
                    co2: edge.co2,
                    time: edge.time
                }]->(end)
            ''', edges=batch)
            
    print("Ingestion to Neo4j Complete!")
    driver.close()

if __name__ == "__main__":
    ingest_to_neo4j()
