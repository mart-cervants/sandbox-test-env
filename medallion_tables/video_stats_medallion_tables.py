from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, current_timestamp, current_date, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, DateType

spark = SparkSession.builder \
    .appName('video_stats_medallion_tables_creation') \
    .getOrCreate()

bucket_name = "yt_statistics"
path = f"gs://{bucket_name}/videos-stats.csv"

schema = StructType([\
        StructField("number", IntegerType()), \
        StructField("title", StringType()), \
        StructField("video_id", StringType()), \
        StructField("publish_date", DateType()), \
        StructField("keyword", StringType()), \
        StructField("likes", DoubleType()), \
        StructField("comments", DoubleType()), \
        StructField("views", DoubleType()), \
    ])

# original CSV with 1881 rows
landing_table = spark.read.csv(path, header=True, schema=schema, multiLine=True)
print("landing_table_count:", landing_table.count())

# spark.sql('''
#     CREATE TABLE IF NOT EXISTS comments_bronze (
#         number INTEGER,
#         video_id STRING,
#         comment STRING,
#         likes INTEGER,
#         sentiment INTEGER,
#         ingested_datetime TIMESTAMP,
#         ingested_date DATE
#     )
#     USING PARQUET
#     PARTITIONED BY (ingested_date)
# ''')
# bronze = landing_table.withColumn("ingested_datetime", current_timestamp()) \
#             .withColumn("ingested_date", current_date())

# bronze.write.insertInto("comments_bronze", overwrite=True)

# # spark.sql("SELECT * FROM comments_bronze").show(30)

# # creation of silver table
# spark.sql('''
#     CREATE TABLE IF NOT EXISTS comments_silver (
#         number INTEGER,
#         video_id STRING,
#         comment STRING,
#         likes INTEGER,
#         sentiment INTEGER,
#         ingested_datetime TIMESTAMP,
#         ingested_date DATE
#     )
#     USING PARQUET
#     PARTITIONED BY (ingested_date)
# ''')

# silver = bronze.dropna()

# # identify row with special characters
# # silver_special_chars = silver.filter(silver.comment.rlike("[^\x00-\x7F]|[\n]"))

# # removing special characters
# silver = silver.withColumn("comment", regexp_replace("comment", "[^\x00-\x7F]|[\n]", ""))
# # silver = silver.filter(length(col("comment")) < 10)

# # 17487 | 1433 row dropedd, probably we are having 397 row with null values without considering with the bad format
# # print("silver_count:", silver.count())


# # create the dead letter table (dead letter: this concept is more related to streaming processing)
# dead_letter_table = bronze.filter(col("number").isNull() | col("video_id").isNull() | col("comment").isNull() | col("likes").isNull() | col("sentiment").isNull())


# silver.write.insertInto("comments_silver", overwrite=True)
# # spark.sql("SELECT * FROM comments_silver").show(30)


# # creation of gold table for videos with possitive sentiment count
# spark.sql('''
#     CREATE TABLE IF NOT EXISTS gold_videos_with_possitive_sentiment_count (
#         video_id STRING,
#         count INTEGER
#     )
#     USING PARQUET
# ''')

# gold_videos_with_possitive_sentiment_count = silver.filter(col("sentiment") == 2.0).select(col("video_id")).groupBy("video_id").count()


# gold_videos_with_possitive_sentiment_count.write.insertInto("gold_videos_with_possitive_sentiment_count", overwrite=True)
# # spark.sql("SELECT * FROM gold_videos_with_possitive_sentiment_count").show(30)