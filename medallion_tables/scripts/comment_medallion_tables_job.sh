gcloud dataproc jobs submit pyspark ./medallion_tables/comments_medallion_tables.py  \
    --cluster=data-sandbox \
    --region=us-central1 \