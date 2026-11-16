import os
import pandas as pd
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", os.environ["NEO4J_PASSWORD"]))

df = pd.read_csv("taxi_trips_clean.csv")

session = driver.session()

for i, row in df.iterrows():
    session.run("""
        MERGE (d:Driver {driver_id: $driver_id})
        MERGE (c:Company {name: $company})
        MERGE (a:Area {area_id: $area_id})
        MERGE (d)-[:WORKS_FOR]->(c)
        CREATE (d)-[:TRIP {trip_id: $trip_id, fare: $fare, trip_seconds: $trip_seconds}]->(a)
    """, driver_id=str(row["driver_id"]), company=str(row["company"]),
         area_id=int(row["dropoff_area"]), trip_id=str(row["trip_id"]),
         fare=float(row["fare"]), trip_seconds=int(row["trip_seconds"]))

session.close()
driver.close()
print("done loading")
