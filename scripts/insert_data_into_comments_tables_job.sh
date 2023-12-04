gcloud dataproc jobs submit pyspark ./scripts/insert_data_into_medallion_comment_tables.py \
    --cluster=data-sandbox \
    --region=us-central1 \