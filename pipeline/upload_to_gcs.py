from google.cloud import storage
import os

def upload_to_gcs(bucket_name, source_file, destination_blob):
    """로컬 파일을 GCS에 업로드"""
    
    # 서비스 계정 키 경로 설정
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'infra/terraform/credentials.json'
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    
    blob.upload_from_filename(source_file)
    print(f"업로드 완료: {source_file} → gs://{bucket_name}/{destination_blob}")

if __name__ == '__main__':
    upload_to_gcs(
        bucket_name='mes-sensor-pipeline-data-lake',
        source_file='data/raw/sensor.csv',
        destination_blob='raw/sensor.csv'
    )