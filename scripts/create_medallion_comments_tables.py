from pyspark.sql import SparkSession

def create_bronze_or_silver_comments_table(table_name):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            number INTEGER,
            video_id STRING,
            comment STRING,
            likes INTEGER,
            sentiment INTEGER,
            ingested_datetime TIMESTAMP,
            ingested_date DATE
        )
        USING PARQUET
        PARTITIONED BY (ingested_date)
    ''')

def create_gold_comments_table(table_name):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    # creation of gold table for videos with possitive sentiment count
    spark.sql(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            video_id STRING,
            count INTEGER
        )
        USING PARQUET
    ''')

spark = SparkSession.builder \
    .appName('bronze_and_silver_comments_tables_creation') \
    .getOrCreate()

bronze_table = "comments_bronze"
silver_table = "comments_silver"
gold_table = "gold_videos_with_possitive_sentiment_count"

create_bronze_or_silver_comments_table(bronze_table)
print(f"Bronze table created with the name: {bronze_table}")
create_bronze_or_silver_comments_table(silver_table)
print(f"Silver table created with the name: {silver_table}")
create_gold_comments_table(gold_table)
print(f"Gold table created with the name: {gold_table}")