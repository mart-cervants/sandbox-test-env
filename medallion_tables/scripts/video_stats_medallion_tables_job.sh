gcloud dataproc jobs submit pyspark ./medallion_tables/video_stats_medallion_tables.py  \
    --cluster=data-sandbox \
    --region=us-central1 \