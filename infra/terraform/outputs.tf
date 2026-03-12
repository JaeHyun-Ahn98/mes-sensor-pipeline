output "bucket_name" {
  description = "생성된 GCS 버킷 이름"
  value       = google_storage_bucket.data_lake.name
}

output "bigquery_dataset" {
  description = "생성된 BigQuery 데이터셋 ID"
  value       = google_bigquery_dataset.sensor_dataset.dataset_id
}