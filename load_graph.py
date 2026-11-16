import os
import pandas as pd
from neo4j import GraphDatabase

graph_db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", os.environ["NEO4J_PASSWORD"]))

taxi_rows = pd.read_csv("taxi_trips_clean.csv")

sess = graph_db.session()

for _, trip in taxi_rows.iterrows():
    sess.run("""
        MERGE (d:Driver {driver_id: $driver_id})
        MERGE (c:Company {name: $company})
        MERGE (a:Area {area_id: $area_id})
        MERGE (d)-[:WORKS_FOR]->(c)
        CREATE (d)-[:TRIP {trip_id: $trip_id, fare: $fare, trip_seconds: $trip_seconds}]->(a)
    """, driver_id=str(trip["driver_id"]), company=str(trip["company"]),
         area_id=int(trip["dropoff_area"]), trip_id=str(trip["trip_id"]),
         fare=float(trip["fare"]), trip_seconds=int(trip["trip_seconds"]))

sess.close()
graph_db.close()
print("done loading")
