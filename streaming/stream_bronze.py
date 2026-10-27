import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

MODE = sys.argv[1] if len(sys.argv) > 1 else "batch"   # batch ou stream

spark = (SparkSession.builder.appName("shopflow-bronze")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", StringType()),
    StructField("purchase_ts", StringType()),
    StructField("is_late_truth", IntegerType()),
    StructField("purchase_day_of_week", IntegerType()),
    StructField("purchase_hour", IntegerType()),
    StructField("item_count", IntegerType()),
    StructField("total_price", DoubleType()),
    StructField("total_freight", DoubleType()),
    StructField("seller_count", IntegerType()),
    StructField("seller_state_count", IntegerType()),
    StructField("same_state_seller_ratio", DoubleType()),
    StructField("avg_product_weight_g", DoubleType()),
    StructField("max_product_weight_g", DoubleType()),
    StructField("avg_product_length_cm", DoubleType()),
    StructField("avg_product_height_cm", DoubleType()),
    StructField("avg_product_width_cm", DoubleType()),
    StructField("customer_state", StringType()),
    StructField("estimated_delivery_days", IntegerType()),
    StructField("avg_distance_km", DoubleType()),
    StructField("distance_missing", IntegerType()),
    StructField("event_time", StringType()),
])

def parse(raw):
    return (raw.select(F.col("value").cast("string").alias("json"),
                       F.col("partition"), F.col("offset"),
                       F.col("timestamp").alias("kafka_ts"))
            .withColumn("d", F.from_json("json", schema))
            .select("d.*", "partition", "offset", "kafka_ts")
            .withColumn("ingested_at", F.current_timestamp()))

OPTS = {"kafka.bootstrap.servers": "localhost:9092", "subscribe": "orders",
        "startingOffsets": "earliest"}

if MODE == "batch":
    raw = spark.read.format("kafka").options(**OPTS).load()
    bronze = parse(raw)
    print("Lignes lues :", bronze.count())
    print("order_id nuls :", bronze.filter("order_id IS NULL").count())
    print("order_id distincts :", bronze.select("order_id").distinct().count())
    bronze.select("order_id", "customer_state", "estimated_delivery_days",
                  "avg_distance_km", "is_late_truth").show(5, truncate=False)
    bronze.write.format("delta").mode("overwrite").save("/work/lakehouse/bronze_orders")
    print("Écrit dans /work/lakehouse/bronze_orders")
else:
    raw = spark.readStream.format("kafka").options(**OPTS).load()
    (parse(raw).writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", "/work/lakehouse/_chk/bronze")
        .trigger(processingTime="5 seconds")
        .start("/work/lakehouse/bronze_orders").awaitTermination())
