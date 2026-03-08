import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")

app = FastAPI(title="Multimodal Routing API (Neo4j)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

driver = None

@app.on_event("startup")
def startup_event():
    global driver
    print("Connecting to Neo4j Database...")
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

@app.on_event("shutdown")
def shutdown_event():
    if driver:
        driver.close()

class RouteRequest(BaseModel):
    start_stop_id: str
    end_stop_id: str
    time_vs_co2_weight: float = 0.5 # 0.0 = only time, 1.0 = only co2
    algorithm: str = "dijkstra"

@app.get("/stops")
def get_stops():
    with driver.session() as session:
        # Get all stops that have at least one route connected
        result = session.run('''
            MATCH (s:Stop)
            WHERE EXISTS((s)-[:ROUTES_TO]-())
            RETURN s.id AS id, s.name AS name
            ORDER BY s.name
        ''')
        return [{"stop_id": record["id"], "stop_name": record["name"]} for record in result]

@app.post("/route")
def calculate_route(req: RouteRequest):
    alpha = 1.0 - req.time_vs_co2_weight
    beta = req.time_vs_co2_weight
    
    # Da normale Pfadsuche bei 100.000 Edges auf Neo4j ohne Index/GDS abstürzt (hangs),
    # nutzen wir apoc.algo.dijkstra (APOC Core ist in Aura Free enabled).
    # Wir müssen vorher sicherstellen, dass wir eine property haben, auf der gesucht wird.
    # Da wir edge.time und edge.co2 erst im query dynamisch verknüpfen wollen,
    # machen wir hierfür vorab einen kleinen Trick oder nutzen raw cypher, wenn APOC dynamische Weights nicht mag.
    
    # Alternativ: Neo4js Dijkstra über GDS (falls installiert) oder APOC dijkstra mit property name.
    # Wir machen zur Sicherheit einen harten Cypher Fallback mit kürzester Distanz (APOC dijkstra)
    # und falls wir keine dynamische Property bauen können, speichern wir the "cost" at query time.
    
    # Wir begrenzen das Suchgebiet, in dem wir node Count Limits übergeben, 
    # aber besser ist es, den Pathfinding Prozess mit apoc.path.expandConfig abzuwickeln
    # Da wir eine Demo machen wollen, geben wir den weight Wert in der DB fix als query constraint.
    # Da apoc.algo.dijkstra nur EINE Eigenschaft als Weight akzeptiert, berechnen wir die Kosten 
    # für Zeit vs CO2 on-the-fly, falls wir das in Neo4j 5 GDS machen. 
    # Für apoc.algo.dijkstra nehmen wir einfach "time" oder "co2" basierend auf der user präferenz
    # als vereinfachte Heuristik!
    
    # Weight field decision:
    weight_field = "co2" if beta > 0.6 else "time"
    
    query = f'''
        MATCH (start:Stop {{id: $start_id}})
        MATCH (end:Stop {{id: $end_id}})
        CALL apoc.algo.dijkstra(start, end, "ROUTES_TO>", "{weight_field}") YIELD path, weight
        WITH path
        WITH nodes(path) as ns, relationships(path) as rels
        WITH ns, rels,
             reduce(cost = 0.0, rel in rels | cost + ($alpha * rel.time) + ($beta * (rel.co2 / 10.0))) AS total_cost
        RETURN 

            [n in ns | n.name] AS stop_names,
            [r in rels | {{
                line: r.line, 
                type: r.type, 
                time: r.time, 
                distance: r.distance, 
                co2: r.co2
            }}] AS path_edges,
            total_cost
    '''
    
    with driver.session() as session:
        try:
            result = session.run(query, start_id=req.start_stop_id, end_id=req.end_stop_id, alpha=alpha, beta=beta)
            record = result.single()
            
            if not record:
                return {"error": "Keine Route gefunden oder zu weit entfernt."}
                
            stop_names = record["stop_names"]
            path_edges = record["path_edges"]
            
            steps = []
            total_time = 0.0
            total_dist = 0.0
            total_co2 = 0.0
            
            for i, edge in enumerate(path_edges):
                steps.append({
                    "from": stop_names[i],
                    "to": stop_names[i+1],
                    "line": edge["line"],
                    "type": edge["type"],
                    "time": round(edge["time"], 1),
                    "distance": round(edge["distance"], 2),
                    "co2": round(edge["co2"], 1)
                })
                total_time += edge["time"]
                total_dist += edge["distance"]
                total_co2 += edge["co2"]
                
            return {
                "summary": {
                    "totalTime": round(total_time, 1),
                    "totalDistance": round(total_dist, 2),
                    "totalCo2": round(total_co2, 1),
                    "transfers": max(0, len(steps) - 1)
                },
                "steps": steps
            }
        except Exception as e:
            print("Error in Route query:", e)
            return {"error": "Berechnung dauerte zu lange oder APOC fehlt. Bitte Neo4j Instanz prüfen."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
