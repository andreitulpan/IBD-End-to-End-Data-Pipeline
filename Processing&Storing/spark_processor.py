import atexit
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    window, avg, max as spark_max, stddev
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

INFLUX_URL = "https://influxdb.ibd.thezion.one"
INFLUX_ORG = "my-org"
INFLUX_BUCKET = "weather_data"
INFLUX_TOKEN = "my-super-secret-token"

influx_client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = influx_client.write_api(write_options=SYNCHRONOUS)

atexit.register(influx_client.close)

BROKER_URL = 'ibd.thezion.one:9092'
USERNAME = 'consumer'
PASSWORD = 'HaiProducatorii_2009'
CA_FILE = 'cf_root_ca.pem'

spark = (
    SparkSession.builder
    .appName("WeatherStreamProcessor")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BROKER_URL)
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")

    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")

    .option(
        "kafka.sasl.jaas.config",
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{USERNAME}" password="{PASSWORD}";'
    )

    #.option("kafka.ssl.ca.location", CA_FILE)
    # Tell Java to use a PEM file as the TrustStore
    .option("kafka.ssl.truststore.type", "PEM")
    # Use the ABSOLUTE path inside the container
    .option("kafka.ssl.truststore.location", f"/app/{CA_FILE}")
    .option("kafka.ssl.endpoint.identification.algorithm", "https")

    .load()
)

weather_schema = StructType([
    StructField("timestamp", StringType(), True),

    StructField("location", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("timezone_abbreviation", StringType(), True),
    ])),

    StructField("current_conditions", StructType([
        StructField("temperature", StructType([
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("apparent", DoubleType(), True),
        ])),
        StructField("humidity", StructType([
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
        ])),
        StructField("wind", StructType([
            StructField("speed", DoubleType(), True),
            StructField("direction", DoubleType(), True),
            StructField("gusts", DoubleType(), True),
            StructField("unit", StringType(), True),
        ])),
        StructField("precipitation", StructType([
            StructField("total", DoubleType(), True),
            StructField("rain", DoubleType(), True),
            StructField("showers", DoubleType(), True),
            StructField("snowfall", DoubleType(), True),
            StructField("unit", StringType(), True),
        ])),
        StructField("atmosphere", StructType([
            StructField("cloud_cover", DoubleType(), True),
            StructField("pressure_msl", DoubleType(), True),
            StructField("surface_pressure", DoubleType(), True),
            StructField("unit_pressure", StringType(), True),
        ])),
        StructField("weather_code", DoubleType(), True),
        StructField("is_day", BooleanType(), True),
    ])),

    StructField("metadata", StructType([
        StructField("iteration", DoubleType(), True),
        StructField("last_api_update", StringType(), True),
        StructField("simulation_mode", StringType(), True),
    ]))
])

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), weather_schema).alias("data"))
    .select(
        to_timestamp(col("data.timestamp")).alias("event_time"),

        col("data.location.latitude").alias("lat"),
        col("data.location.longitude").alias("lon"),

        col("data.current_conditions.temperature.value").alias("temperature_c"),
        col("data.current_conditions.temperature.apparent").alias("apparent_temperature_c"),

        col("data.current_conditions.humidity.value").alias("humidity_pct"),

        col("data.current_conditions.wind.speed").alias("wind_speed_kmph"),
        col("data.current_conditions.wind.gusts").alias("wind_gust_kmph"),
        col("data.current_conditions.wind.direction").alias("wind_direction_deg"),

        col("data.current_conditions.atmosphere.pressure_msl").alias("pressure_hpa"),
        col("data.current_conditions.atmosphere.cloud_cover").alias("cloud_cover_pct"),

        col("data.current_conditions.precipitation.total").alias("precipitation_mm")
    )
)

aggregated_df = (
    parsed_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes")
    )
    .agg(
        avg("temperature_c").alias("avg_temperature_c"),
        avg("apparent_temperature_c").alias("avg_apparent_temperature_c"),
        stddev("temperature_c").alias("temperature_stddev"),

        avg("wind_speed_kmph").alias("avg_wind_speed_kmph"),
        spark_max("wind_gust_kmph").alias("max_wind_gust_kmph"),

        avg("pressure_hpa").alias("avg_pressure_hpa"),
        avg("humidity_pct").alias("avg_humidity_pct"),
        avg("precipitation_mm").alias("total_precipitation_mm")
    )
)

def write_to_influx(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    points = []

    for row in rows:
        ts = row.window.end

        point = (
            Point("weather_metrics_5m")
            .tag("location", "Bucharest")
            .tag("window", "5m")
            .field("avg_temperature_c", float(row.avg_temperature_c))
            .field("avg_apparent_temperature_c", float(row.avg_apparent_temperature_c))
            .field("temperature_stddev", float(row.temperature_stddev))
            .field("avg_wind_speed_kmph", float(row.avg_wind_speed_kmph))
            .field("max_wind_gust_kmph", float(row.max_wind_gust_kmph))
            .field("avg_pressure_hpa", float(row.avg_pressure_hpa))
            .field("avg_humidity_pct", float(row.avg_humidity_pct))
            .field("total_precipitation_mm", float(row.total_precipitation_mm))
            .time(ts, WritePrecision.NS)
        )

        points.append(point)

    write_api.write(
        bucket=INFLUX_BUCKET,
        record=points
    )



query = (
    aggregated_df
    .writeStream
    .outputMode("update")
    .foreachBatch(write_to_influx)
    .option("checkpointLocation", "/tmp/spark/weather_checkpoint")
    .start()
)

query.awaitTermination()
