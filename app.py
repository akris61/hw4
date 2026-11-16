import os
from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = Flask(__name__)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", os.environ["NEO4J_PASSWORD"]))

spark = (
    SparkSession.builder
    .appName("HW4API")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)


# neo4j endpoints

@app.get("/graph-summary")
def graph_summary():
    session = driver.session()
    drivers = session.run("MATCH (d:Driver) RETURN count(d) AS c").single()["c"]
    companies = session.run("MATCH (c:Company) RETURN count(c) AS c").single()["c"]
    areas = session.run("MATCH (a:Area) RETURN count(a) AS c").single()["c"]
    trips = session.run("MATCH ()-[t:TRIP]->() RETURN count(t) AS c").single()["c"]
    session.close()
    return jsonify({"driver_count": drivers, "company_count": companies,
                    "area_count": areas, "trip_count": trips})


@app.get("/top-companies")
def top_companies():
    n = int(request.args.get("n"))
    session = driver.session()
    result = session.run("""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
        RETURN c.name AS name, count(t) AS trip_count
        ORDER BY trip_count DESC
        LIMIT $n
    """, n=n)
    companies = []
    for r in result:
        companies.append({"name": r["name"], "trip_count": r["trip_count"]})
    session.close()
    return jsonify({"companies": companies})


@app.get("/high-fare-trips")
def high_fare_trips():
    area_id = int(request.args.get("area_id"))
    min_fare = float(request.args.get("min_fare"))
    session = driver.session()
    result = session.run("""
        MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
        WHERE t.fare > $min_fare
        RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
        ORDER BY fare DESC, trip_id
    """, area_id=area_id, min_fare=min_fare)
    trips = []
    for r in result:
        trips.append({"trip_id": r["trip_id"], "fare": r["fare"], "driver_id": r["driver_id"]})
    session.close()
    return jsonify({"trips": trips})


@app.get("/co-area-drivers")
def co_area_drivers():
    driver_id = request.args.get("driver_id")
    session = driver.session()
    result = session.run("""
        MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
        WHERE d1 <> d2
        RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
        ORDER BY shared_areas DESC, driver_id
    """, driver_id=driver_id)
    drivers = []
    for r in result:
        drivers.append({"driver_id": r["driver_id"], "shared_areas": r["shared_areas"]})
    session.close()
    return jsonify({"co_area_drivers": drivers})


@app.get("/avg-fare-by-company")
def avg_fare_by_company():
    session = driver.session()
    result = session.run("""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
        RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
        ORDER BY avg_fare DESC
    """)
    companies = []
    for r in result:
        companies.append({"name": r["name"], "avg_fare": r["avg_fare"]})
    session.close()
    return jsonify({"companies": companies})


# spark endpoints

@app.get("/area-stats")
def area_stats():
    area_id = int(request.args.get("area_id"))
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    df = df.filter(df["dropoff_area"] == area_id)
    stats = df.groupBy("dropoff_area").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare"), 2).alias("avg_fare"),
        F.round(F.avg("trip_seconds"), 2).alias("avg_trip_seconds")
    ).collect()

    if len(stats) == 0:
        return jsonify({"area_id": area_id, "trip_count": 0, "avg_fare": 0, "avg_trip_seconds": 0})

    row = stats[0]
    return jsonify({"area_id": area_id, "trip_count": row["trip_count"],
                    "avg_fare": row["avg_fare"], "avg_trip_seconds": row["avg_trip_seconds"]})


@app.get("/top-pickup-areas")
def top_pickup_areas():
    n = int(request.args.get("n"))
    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    rows = df.groupBy("pickup_area").agg(F.count("*").alias("trip_count")) \
             .orderBy(F.desc("trip_count")).limit(n).collect()
    areas = []
    for r in rows:
        areas.append({"pickup_area": r["pickup_area"], "trip_count": r["trip_count"]})
    return jsonify({"areas": areas})


@app.get("/company-compare")
def company_compare():
    company1 = request.args.get("company1")
    company2 = request.args.get("company2")

    df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
    df = df.withColumn("fare_per_minute", df["fare"] / (df["trip_seconds"] / 60.0))
    df.createOrReplaceTempView("trips")

    rows = spark.sql("""
        SELECT company, COUNT(*) AS trip_count, ROUND(AVG(fare), 2) AS avg_fare,
        ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
        ROUND(AVG(trip_seconds), 2) AS avg_trip_seconds
        FROM trips
        WHERE company IN ('""" + company1 + "', '" + company2 + """')
        GROUP BY company
    """).collect()

    found = {}
    for r in rows:
        found[r["company"]] = r

    if company1 not in found or company2 not in found:
        return jsonify({"error": "one or more companies not found"})

    comparison = []
    for name in [company1, company2]:
        r = found[name]
        comparison.append({
            "company": name,
            "trip_count": r["trip_count"],
            "avg_fare": r["avg_fare"],
            "avg_fare_per_minute": r["avg_fare_per_minute"],
            "avg_trip_seconds": r["avg_trip_seconds"]
        })
    return jsonify({"comparison": comparison})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=False)
