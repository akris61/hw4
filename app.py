import os
from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

app = Flask(__name__)

graph_db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", os.environ["NEO4J_PASSWORD"]))

spark = (
    SparkSession.builder
    .appName("HW4API")
    .master("local[1]")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

TRIPS_CSV = "taxi_trips_clean.csv"


def load_trips():
    return spark.read.csv(TRIPS_CSV, header=True, inferSchema=True)


# neo4j endpoints

@app.get("/graph-summary")
def graph_summary():
    sess = graph_db.session()
    num_drivers = sess.run("MATCH (d:Driver) RETURN count(d) AS c").single()["c"]
    num_companies = sess.run("MATCH (c:Company) RETURN count(c) AS c").single()["c"]
    num_areas = sess.run("MATCH (a:Area) RETURN count(a) AS c").single()["c"]
    num_trips = sess.run("MATCH ()-[t:TRIP]->() RETURN count(t) AS c").single()["c"]
    sess.close()
    return jsonify({"driver_count": num_drivers, "company_count": num_companies,
                    "area_count": num_areas, "trip_count": num_trips})


@app.get("/top-companies")
def top_companies():
    limit_n = int(request.args.get("n"))
    sess = graph_db.session()
    records = sess.run("""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
        RETURN c.name AS name, count(t) AS trip_count
        ORDER BY trip_count DESC
        LIMIT $n
    """, n=limit_n)
    top_list = [{"name": rec["name"], "trip_count": rec["trip_count"]} for rec in records]
    sess.close()
    return jsonify({"companies": top_list})


@app.get("/high-fare-trips")
def high_fare_trips():
    target_area = int(request.args.get("area_id"))
    fare_floor = float(request.args.get("min_fare"))
    sess = graph_db.session()
    records = sess.run("""
        MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
        WHERE t.fare > $min_fare
        RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
        ORDER BY fare DESC, trip_id
    """, area_id=target_area, min_fare=fare_floor)
    pricey_trips = [{"trip_id": rec["trip_id"], "fare": rec["fare"], "driver_id": rec["driver_id"]}
                    for rec in records]
    sess.close()
    return jsonify({"trips": pricey_trips})


@app.get("/co-area-drivers")
def co_area_drivers():
    base_driver = request.args.get("driver_id")
    sess = graph_db.session()
    records = sess.run("""
        MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
        WHERE d1 <> d2
        RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
        ORDER BY shared_areas DESC, driver_id
    """, driver_id=base_driver)
    neighbors = [{"driver_id": rec["driver_id"], "shared_areas": rec["shared_areas"]} for rec in records]
    sess.close()
    return jsonify({"co_area_drivers": neighbors})


@app.get("/avg-fare-by-company")
def avg_fare_by_company():
    sess = graph_db.session()
    records = sess.run("""
        MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(a:Area)
        RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
        ORDER BY avg_fare DESC
    """)
    fare_rows = [{"name": rec["name"], "avg_fare": rec["avg_fare"]} for rec in records]
    sess.close()
    return jsonify({"companies": fare_rows})


# spark endpoints

@app.get("/area-stats")
def area_stats():
    target_area = int(request.args.get("area_id"))
    trips_df = load_trips()
    area_df = trips_df.filter(trips_df["dropoff_area"] == target_area)
    summary = area_df.groupBy("dropoff_area").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare"), 2).alias("avg_fare"),
        F.round(F.avg("trip_seconds"), 2).alias("avg_trip_seconds")
    ).collect()

    if not summary:
        return jsonify({"area_id": target_area, "trip_count": 0, "avg_fare": 0, "avg_trip_seconds": 0})

    agg_row = summary[0]
    return jsonify({"area_id": target_area, "trip_count": agg_row["trip_count"],
                    "avg_fare": agg_row["avg_fare"], "avg_trip_seconds": agg_row["avg_trip_seconds"]})


@app.get("/top-pickup-areas")
def top_pickup_areas():
    limit_n = int(request.args.get("n"))
    trips_df = load_trips()
    ranked = trips_df.groupBy("pickup_area").agg(F.count("*").alias("trip_count")) \
                     .orderBy(F.desc("trip_count")).limit(limit_n).collect()
    pickup_list = [{"pickup_area": rec["pickup_area"], "trip_count": rec["trip_count"]} for rec in ranked]
    return jsonify({"areas": pickup_list})


@app.get("/company-compare")
def company_compare():
    first_co = request.args.get("company1")
    second_co = request.args.get("company2")

    trips_df = load_trips()
    trips_df = trips_df.withColumn("fare_per_minute", trips_df["fare"] / (trips_df["trip_seconds"] / 60.0))
    trips_df.createOrReplaceTempView("trips")

    metric_rows = spark.sql("""
        SELECT company, COUNT(*) AS trip_count, ROUND(AVG(fare), 2) AS avg_fare,
        ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
        ROUND(AVG(trip_seconds), 2) AS avg_trip_seconds
        FROM trips
        WHERE company IN ('""" + first_co + "', '" + second_co + """')
        GROUP BY company
    """).collect()

    by_company = {rec["company"]: rec for rec in metric_rows}

    if first_co not in by_company or second_co not in by_company:
        return jsonify({"error": "one or more companies not found"})

    side_by_side = []
    for co_name in [first_co, second_co]:
        rec = by_company[co_name]
        side_by_side.append({
            "company": co_name,
            "trip_count": rec["trip_count"],
            "avg_fare": rec["avg_fare"],
            "avg_fare_per_minute": rec["avg_fare_per_minute"],
            "avg_trip_seconds": rec["avg_trip_seconds"]
        })
    return jsonify({"comparison": side_by_side})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=False)
