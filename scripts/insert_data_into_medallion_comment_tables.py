from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, current_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def create_landing_table(path, schema):
    landing_table = spark.read.csv(path, header=True, schema=schema, multiLine=True)
    return landing_table

def insert_into_bronze_comments_table(landing_table):
    bronze = landing_table.withColumn("ingested_datetime", current_timestamp()) \
            .withColumn("ingested_date", current_date())

    bronze.write.insertInto("comments_bronze", overwrite=True)

    return bronze

def insert_into_silver_comments_table(bronze_table):
    silver = bronze_table.dropna()

    # removing special characters
    silver = silver.withColumn("comment", regexp_replace("comment", "[^\x00-\x7F]|[\n]", ""))

    silver.write.insertInto("comments_silver", overwrite=True)
    return silver


# create the dead letter table (dead letter: this concept is more related to streaming processing)
# dead_letter_table = bronze.filter(col("number").isNull() | col("video_id").isNull() | col("comment").isNull() | col("likes").isNull() | col("sentiment").isNull())


def insert_into_gold_comments_table(silve_table):
    gold_videos_with_possitive_sentiment_count = silve_table.filter(col("sentiment") == 2.0).select(col("video_id")).groupBy("video_id").count()
    gold_videos_with_possitive_sentiment_count.write.insertInto("gold_videos_with_possitive_sentiment_count", overwrite=True)

spark = SparkSession.builder \
    .appName('insert_into_comment_medallion_tables') \
    .getOrCreate()

bucket_name = "yt_statistics"
path = f"gs://{bucket_name}/comments.csv"

schema = StructType([\
        StructField("number", IntegerType()), \
        StructField("video_id", StringType()), \
        StructField("comment", StringType()), \
        StructField("likes", DoubleType()), \
        StructField("sentiment", DoubleType()), \
    ])


landing_table = create_landing_table(path, schema)
print("Landing table created")
bronze = insert_into_bronze_comments_table(landing_table)
print("Data inserted in bronze table")
silver = insert_into_silver_comments_table(bronze)
print("Data inserted in silver table")
insert_into_gold_comments_table(silver)
print("Data inserted in gold table")
