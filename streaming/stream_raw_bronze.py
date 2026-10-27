import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

MODE = sys.argv[1] if len(sys.argv) > 1 else "batch"   # batch ou stream

spark = (SparkSession.builder.appName("shopflow-raw-bronze")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

item = StructType([
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("price", DoubleType()),
    StructField("freight_value", DoubleType())])

schema = StructType([
    StructField("order_id", StringType()),
    StructField("purchase_ts", StringType()),
    StructField("customer_zip", StringType()),
    StructField("customer_state", StringType()),
    StructField("estimated_delivery_date", StringType()),
    StructField("items", ArrayType(item))])

def parse(raw):
    return (raw.select(F.col("value").cast("string").alias("json"),
                       F.col("partition"), F.col("offset"),
                       F.col("timestamp").alias("kafka_ts"))
            .withColumn("d", F.from_json("json", schema))
            .select("d.*", "json", "partition", "offset", "kafka_ts")
            .withColumn("ingested_at", F.current_timestamp()))

OPTS = {"kafka.bootstrap.servers": "localhost:9092",
        "subscribe": "orders_raw", "startingOffsets": "earliest"}
OUT = "/work/lakehouse/bronze_orders_raw"

if MODE == "batch":
    b = parse(spark.read.format("kafka").options(**OPTS).load())
    print("Lignes lues :", b.count())
    print("order_id nuls :", b.filter("order_id IS NULL").count())
    print("order_id distincts :", b.select("order_id").distinct().count())
    print("Commandes sans articles :", b.filter("size(items) = 0").count())
    b.selectExpr("order_id", "customer_state", "size(items) as n_items").show(5, truncate=False)
    b.write.format("delta").mode("overwrite").save(OUT)
    print("Écrit dans", OUT)
else:
    (parse(spark.readStream.format("kafka").options(**OPTS).load())
        .writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", "/work/lakehouse/_chk/bronze_raw")
        .trigger(processingTime="5 seconds").start(OUT).awaitTermination())
