from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ----------------------------------------
# 1. CREATE SPARK SESSION
# ----------------------------------------
spark = SparkSession.builder \
    .appName("WikiPipeline") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------
# 2. READ FROM KAFKA STREAM
# ----------------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "wiki_edits") \
    .option("startingOffsets", "latest") \
    .load()

# ----------------------------------------
# 3. RAW STREAM (FIXED ID ISSUE)
# ----------------------------------------
raw_df = df.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "timestamp as event_time",
    "offset as kafka_id"
)

# ----------------------------------------
# 4. DEFINE JSON SCHEMA
# ----------------------------------------
schema = StructType([
    StructField("meta", StructType([
        StructField("dt", StringType()),
        StructField("id", StringType()),
        StructField("uri", StringType())
    ])),
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("wiki", StringType()),
    StructField("bot", BooleanType()),
    StructField("length", StructType([
        StructField("old", IntegerType()),
        StructField("new", IntegerType())
    ])),
    StructField("revision", StructType([
        StructField("old", LongType()),
        StructField("new", LongType())
    ])),
    StructField("log_type", StringType()),
    StructField("log_action", StringType())
])

# ----------------------------------------
# 5. PARSE JSON
# ----------------------------------------
json_df = raw_df.select(
    from_json(col("raw_json"), schema).alias("data"),
    col("event_time"),
    col("kafka_id"),
    col("raw_json")
)

parsed_df = json_df.select(
    col("data.id").alias("event_id"),   # JSON id renamed
    col("data.type"),
    col("data.title"),
    col("data.user"),
    col("data.wiki"),
    col("data.bot"),
    col("data.length"),
    col("data.revision"),
    col("data.log_type"),
    col("data.log_action"),
    col("event_time"),
    col("kafka_id").alias("id"),        # Kafka id (primary key)
    col("raw_json")
)

parsed_df.printSchema()

# ----------------------------------------
# 6. BASE TABLE
# ----------------------------------------
base_df = parsed_df.select(
    "event_time", "id", "type", "title", "user", "wiki", "bot"
)

# ----------------------------------------
# 7. EDIT EVENTS
# ----------------------------------------
edit_df = parsed_df.filter(col("type") == "edit").select(
    "title", "event_time", "user", "wiki",
    col("length.old").alias("old_length"),
    col("length.new").alias("new_length"),
    col("revision.old").alias("revision_old"),
    col("revision.new").alias("revision_new")
)

# ----------------------------------------
# 8. LOG EVENTS
# ----------------------------------------
log_df = parsed_df.filter(col("type") == "log").select(
    "title", "event_time", "user", "wiki",
    "log_type", "log_action"
)

# ----------------------------------------
# 9. NEW PAGE EVENTS
# ----------------------------------------
new_df = parsed_df.filter(col("type") == "new").select(
    "title", "event_time", "user", "wiki",
    col("length.new").alias("length")
)

# ----------------------------------------
# 10. WRITE TO CASSANDRA
# ----------------------------------------

# BASE TABLE
base_query = base_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "wiki") \
    .option("table", "events_base") \
    .option("checkpointLocation", "file:///tmp/checkpoint_base") \
    .outputMode("append") \
    .start()

# EDIT TABLE
edit_query = edit_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "wiki") \
    .option("table", "events_edit") \
    .option("checkpointLocation", "file:///tmp/checkpoint_edit") \
    .outputMode("append") \
    .start()

# LOG TABLE
log_query = log_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "wiki") \
    .option("table", "events_log") \
    .option("checkpointLocation", "file:///tmp/checkpoint_log") \
    .outputMode("append") \
    .start()

# NEW TABLE
new_query = new_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "wiki") \
    .option("table", "events_new") \
    .option("checkpointLocation", "file:///tmp/checkpoint_new") \
    .outputMode("append") \
    .start()

# ----------------------------------------
# 11. DEBUG OUTPUT (VERY IMPORTANT)
# ----------------------------------------
console_query = parsed_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()
    
# ----------------------------------------
# RAW EVENTS TABLE
# ----------------------------------------
raw_query = parsed_df.select(
    "event_time", "id", "raw_json").writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "wiki") \
    .option("table", "raw_events") \
    .option("checkpointLocation", "file:///tmp/checkpoint_raw") \
    .outputMode("append") \
    .start()

# ----------------------------------------
# 12. AWAIT TERMINATION
# ----------------------------------------
spark.streams.awaitAnyTermination()
