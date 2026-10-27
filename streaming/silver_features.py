from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("shopflow-silver")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

D = "/work/data/"
bronze = spark.read.format("delta").load("/work/lakehouse/bronze_orders_raw") \
              .dropDuplicates(["order_id"])

prod = (spark.read.csv(D + "ref_products.csv", header=True, inferSchema=True)
        .select("product_id", "weight_g", "length_cm", "height_cm", "width_cm"))
sell = (spark.read.csv(D + "ref_sellers.csv", header=True)
        .select("seller_id", F.col("seller_zip").alias("s_zip"), "seller_state"))
geo = spark.read.csv(D + "zip_coords.csv", header=True, inferSchema=False) \
           .select(F.col("zip"), F.col("lat").cast("double").alias("lat"),
                   F.col("lng").cast("double").alias("lng"))
geo_c = geo.select(F.col("zip").alias("customer_zip"), F.col("lat").alias("c_lat"), F.col("lng").alias("c_lng"))
geo_s = geo.select(F.col("zip").alias("s_zip"), F.col("lat").alias("s_lat"), F.col("lng").alias("s_lng"))

items = (bronze.select("order_id", "purchase_ts", "customer_zip", "customer_state",
                       "estimated_delivery_date", F.explode("items").alias("it"))
         .select("order_id", "purchase_ts", "customer_zip", "customer_state",
                 "estimated_delivery_date", "it.*")
         .join(prod, "product_id", "left")
         .join(sell, "seller_id", "left")
         .join(geo_c, "customer_zip", "left")
         .join(geo_s, "s_zip", "left"))

R = 6371.0
lat1, lat2 = F.radians("c_lat"), F.radians("s_lat")
a = (F.sin((lat2 - lat1) / 2) ** 2 +
     F.cos(lat1) * F.cos(lat2) * F.sin(F.radians(F.col("s_lng") - F.col("c_lng")) / 2) ** 2)
items = (items.withColumn("dist_km", 2 * R * F.asin(F.sqrt(a)))
              .withColumn("same_state", (F.col("seller_state") == F.col("customer_state")).cast("int"))
              .withColumn("w", F.when(F.col("weight_g") == 0, None).otherwise(F.col("weight_g"))))

agg = (items.groupBy("order_id", "purchase_ts", "customer_zip", "customer_state",
                     "estimated_delivery_date")
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
silver = (agg
    .withColumn("purchase_day_of_week", (F.dayofweek(ts) + 5) % 7)
    .withColumn("purchase_hour", F.hour(ts))
    .withColumn("estimated_delivery_days",
                F.datediff(F.to_timestamp("estimated_delivery_date"), ts))
    .withColumn("distance_missing", F.col("avg_distance_km").isNull().cast("int")))

silver.write.format("delta").mode("overwrite").save("/work/lakehouse/silver_orders_features")

s = spark.read.format("delta").load("/work/lakehouse/silver_orders_features")
print("Lignes Silver :", s.count(), "(attendu = lignes Bronze)")
print("distance_missing :", s.filter("distance_missing = 1").count())
print("poids nuls :", s.filter("avg_product_weight_g IS NULL").count())
s.select(F.round(F.avg("estimated_delivery_days"), 1).alias("delai_moy"),
         F.min("estimated_delivery_days").alias("delai_min"),
         F.max("estimated_delivery_days").alias("delai_max"),
         F.round(F.avg("avg_distance_km"), 1).alias("dist_moy"),
         F.round(F.avg("item_count"), 2).alias("items_moy")).show()
s.select("order_id", "customer_state", "item_count", "total_price",
         "avg_distance_km", "estimated_delivery_days").show(5, truncate=False)
