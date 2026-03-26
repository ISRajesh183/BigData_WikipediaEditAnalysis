from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

# -------------------------------
# 1. Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("WikiFullAnalytics") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# -------------------------------
# 2. Load Data
# -------------------------------
events_base = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="events_base", keyspace="wiki") \
    .load()

events_edit = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="events_edit", keyspace="wiki") \
    .load()

# -------------------------------
# 3. BASIC ANALYTICS
# -------------------------------

# A. Wiki counts
wiki_counts = events_base.groupBy("wiki") \
    .agg(count("*").alias("event_count"))

# B. User activity (non-bot)
user_activity = events_base.filter(col("bot") == False) \
    .groupBy("user") \
    .agg(count("*").alias("activity_count"))

# -------------------------------
# 4. EDIT-BASED FEATURES
# -------------------------------
edit_diff = events_edit.withColumn(
    "size_diff",
    abs(col("new_length") - col("old_length"))
)

# -------------------------------
# 5. SUSPICIOUS EDITS
# -------------------------------
suspicious_edits = edit_diff.filter(col("size_diff") > 5000) \
    .select("wiki", "event_time", "title", "user", "size_diff")

# -------------------------------
# 6. Z-SCORE ANOMALIES
# -------------------------------
stats = edit_diff.select(
    mean("size_diff").alias("mean"),
    stddev("size_diff").alias("std")
).collect()[0]

mean_val = stats["mean"]
std_val = stats["std"] if stats["std"] != 0 else 1

anomalies = edit_diff.withColumn(
    "z_score",
    (col("size_diff") - lit(mean_val)) / lit(std_val)
).filter(col("z_score") > 3) \
 .select("wiki", "event_time", "title", "user", "z_score")

# -------------------------------
# 7. TRENDING PAGES
# -------------------------------
trending = events_base.groupBy(
    "title",
    window("event_time", "10 minutes")
).agg(count("*").alias("edit_count")) \
 .filter("edit_count > 5") \
 .select(
    col("title"),
    col("window.start").alias("window_start"),
    col("edit_count")
)

# -------------------------------
# 8. EDIT WAR DETECTION
# -------------------------------
edit_wars = events_base.groupBy(
    "title",
    window("event_time", "5 minutes")
).agg(
    countDistinct("user").alias("unique_users"),
    count("*").alias("total_edits")
).filter(
    (col("unique_users") > 3) & (col("total_edits") > 8)
).select(
    col("title"),
    col("window.start").alias("window_start"),
    col("unique_users"),
    col("total_edits")
)

# -------------------------------
# 9. REVERT DETECTION
# -------------------------------
w = Window.partitionBy("title").orderBy("event_time")

reverts = edit_diff.withColumn(
    "prev_diff", lag("size_diff").over(w)
).filter(
    col("prev_diff").isNotNull() &
    (abs(col("size_diff") - col("prev_diff")) < 50)
)

# -------------------------------
# 10. ML - USER CLUSTERING
# -------------------------------
user_features = edit_diff.groupBy("user").agg(
    avg("size_diff").alias("avg_diff")
).na.fill(0)

assembler = VectorAssembler(
    inputCols=["avg_diff"],
    outputCol="features"
)

data = assembler.transform(user_features)

kmeans = KMeans(k=2, seed=1)
model = kmeans.fit(data)

clusters = model.transform(data) \
    .select("user", col("prediction").alias("cluster"))

# -------------------------------
# 11. WRITE BACK TO CASSANDRA
# -------------------------------

def write(df, table, mode="append"):
    df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace="wiki") \
        .mode("append") \
        .save()

write(wiki_counts, "analytics_wiki_counts", "overwrite")
write(user_activity, "analytics_user_activity", "overwrite")
write(suspicious_edits, "analytics_suspicious_edits")
write(anomalies, "analytics_anomalies")
write(trending, "analytics_trending")
write(edit_wars, "analytics_edit_wars")

print("✅ FULL ANALYTICS PIPELINE COMPLETED")

spark.stop()
