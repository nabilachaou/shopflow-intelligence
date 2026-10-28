import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = (SparkSession.builder.appName("shopflow-stream")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

L = "/work/lakehouse/"
BRONZE, SILVER = L + "bronze_orders_raw", L + "silver_orders_features"
D = "/work/data/"

# ---------- Données de référence (statiques, petites) ----------
prod = F.broadcast(spark.read.csv(D + "ref_products.csv", header=True, inferSchema=True)
        .select("product_id", "weight_g", "length_cm", "height_cm", "width_cm"))
sell = F.broadcast(spark.read.csv(D + "ref_sellers.csv", header=True)
        .select("seller_id", F.col("seller_zip").alias("s_zip"), "seller_state"))
geo = spark.read.csv(D + "zip_coords.csv", header=True) \
        .select("zip", F.col("lat").cast("double").alias("lat"), F.col("lng").cast("double").alias("lng"))
geo_c = F.broadcast(geo.select(F.col("zip").alias("customer_zip"),
                               F.col("lat").alias("c_lat"), F.col("lng").alias("c_lng")))
geo_s = F.broadcast(geo.select(F.col("zip").alias("s_zip"),
                               F.col("lat").alias("s_lat"), F.col("lng").alias("s_lng")))

# ---------- Schéma des messages ----------
item = StructType([StructField("order_item_id", IntegerType()), StructField("product_id", StringType()),
                   StructField("seller_id", StringType()), StructField("price", DoubleType()),
                   StructField("freight_value", DoubleType())])
schema = StructType([StructField("order_id", StringType()), StructField("purchase_ts", StringType()),
                     StructField("customer_zip", StringType()), StructField("customer_state", StringType()),
                     StructField("estimated_delivery_date", StringType()),
                     StructField("items", ArrayType(item))])

def merge_into(batch_df, path, view):
    """Écriture idempotente : n'insère que les order_id absents de la cible."""
    batch_df = batch_df.dropDuplicates(["order_id"])
    if not os.path.exists(path + "/_delta_log"):
        batch_df.write.format("delta").mode("append").save(path)
        return
    batch_df.createOrReplaceTempView(view)
    batch_df.sparkSession.sql(
        f"MERGE INTO delta.`{path}` t USING {view} s ON t.order_id = s.order_id "
        f"WHEN NOT MATCHED THEN INSERT *")

# ---------- Requête 1 : Kafka -> Bronze ----------
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", os.environ.get("KAFKA_SERVERS", "localhost:9092"))
       .option("subscribe", "orders_raw").option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 2000).load())
bronze_stream = (raw.select(F.col("value").cast("string").alias("json"),
                            F.col("partition"), F.col("offset"), F.col("timestamp").alias("kafka_ts"))
                 .withColumn("d", F.from_json("json", schema))
                 .select("d.*", "json", "partition", "offset", "kafka_ts")
                 .withColumn("ingested_at", F.current_timestamp())
                 .filter("order_id IS NOT NULL"))
q1 = (bronze_stream.writeStream
      .foreachBatch(lambda df, i: merge_into(df, BRONZE, "b_batch"))
      .option("checkpointLocation", L + "_chk/bronze")
      .trigger(processingTime="5 seconds").start())

# ---------- Requête 2 : Bronze -> Silver (variables du modèle) ----------
def to_silver(df, batch_id):
    if df.isEmpty():
        return
    it = (df.select("order_id", "purchase_ts", "customer_zip", "customer_state",
                    "estimated_delivery_date", F.explode("items").alias("it"))
          .select("order_id", "purchase_ts", "customer_zip", "customer_state",
                  "estimated_delivery_date", "it.*")
          .join(prod, "product_id", "left").join(sell, "seller_id", "left")
          .join(geo_c, "customer_zip", "left").join(geo_s, "s_zip", "left"))
    R = 6371.0
    la1, la2 = F.radians("c_lat"), F.radians("s_lat")
    a = (F.sin((la2 - la1) / 2) ** 2 +
         F.cos(la1) * F.cos(la2) * F.sin(F.radians(F.col("s_lng") - F.col("c_lng")) / 2) ** 2)
    it = (it.withColumn("dist_km", 2 * R * F.asin(F.sqrt(a)))
            .withColumn("same_state", (F.col("seller_state") == F.col("customer_state")).cast("int"))
            .withColumn("w", F.when(F.col("weight_g") == 0, None).otherwise(F.col("weight_g"))))
    agg = (it.groupBy("order_id", "purchase_ts", "customer_zip", "customer_state", "estimated_delivery_date")
           .agg(F.count("*").alias("item_count"),
                F.round(F.sum("price"), 2).alias("total_price"),
                F.round(F.sum("freight_value"), 2).alias("total_freight"),
                F.countDistinct("seller_id").alias("seller_count"),
                F.countDistinct("seller_state").alias("seller_state_count"),
                F.round(F.avg("same_state"), 4).alias("same_state_seller_ratio"),
                F.round(F.avg("w"), 2).alias("avg_product_weight_g"),
                F.max("w").alias("max_product_weight_g"),
                F.round(F.avg("length_cm"), 2).alias("avg_product_length_cm"),
                F.round(F.avg("height_cm"), 2).alias("avg_product_height_cm"),
                F.round(F.avg("width_cm"), 2).alias("avg_product_width_cm"),
                F.round(F.avg("dist_km"), 1).alias("avg_distance_km")))
    ts = F.to_timestamp("purchase_ts")
    silver = (agg.withColumn("purchase_day_of_week", (F.dayofweek(ts) + 5) % 7)
                 .withColumn("purchase_hour", F.hour(ts))
                 .withColumn("estimated_delivery_days",
                             F.datediff(F.to_timestamp("estimated_delivery_date"), ts))
                 .withColumn("distance_missing", F.col("avg_distance_km").isNull().cast("int")))
    merge_into(silver, SILVER, "s_batch")

# Attend que Bronze existe avant de le lire en streaming
import time
def bronze_ready():
    try:
        return spark.read.format("delta").load(BRONZE).count() > 0
    except Exception:
        return False

while not bronze_ready():
    time.sleep(3)

q2 = (spark.readStream.format("delta").load(BRONZE)
      .writeStream.foreachBatch(to_silver)
      .option("checkpointLocation", L + "_chk/silver")
      .trigger(processingTime="10 seconds").start())

spark.streams.awaitAnyTermination()
