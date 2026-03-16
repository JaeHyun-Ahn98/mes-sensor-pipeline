import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'infra/terraform/credentials.json'

BUCKET_NAME = 'mes-sensor-pipeline-data-lake'
BQ_DATASET = 'sensor_data'
BQ_TABLE = 'sensor_daily_stats'
PROJECT_ID = 'mes-sensor-pipeline'

def create_spark_session():
    return SparkSession.builder \
        .appName('SensorBatchProcessing') \
        .config('spark.jars.packages',
                'com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22') \
        .config('spark.hadoop.google.cloud.auth.service.account.enable', 'true') \
        .config('spark.hadoop.google.cloud.auth.service.account.json.keyfile',
                'infra/terraform/credentials.json') \
        .config('spark.hadoop.fs.gs.impl',
                'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem') \
        .getOrCreate()

def process_sensor_data(spark):
    print("1. GCS에서 데이터 읽는 중...")
    df = spark.read \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .csv(f'gs://{BUCKET_NAME}/raw/sensor.csv')

    print(f"   총 행 수: {df.count()}")

    print("2. 불필요한 컬럼 제거...")
    # sensor_15 제거 (100% 결측)
    df = df.drop('sensor_15')

    print("3. 타입 변환 & 새 컬럼 추가...")
    # timestamp 문자열 → datetime 타입 변환
    df = df.withColumn('timestamp', F.to_timestamp('timestamp'))

    # 날짜, 시간 컬럼 분리
    df = df.withColumn('date', F.to_date('timestamp'))
    df = df.withColumn('hour', F.hour('timestamp'))

    # 이상 여부 컬럼 추가
    df = df.withColumn('is_anomaly',
        F.when(F.col('machine_status') == 'NORMAL', 0).otherwise(1))

    print("4. 결측값 처리...")
    # 센서 컬럼 결측값 0으로 채우기
    sensor_cols = [c for c in df.columns if c.startswith('sensor_')]
    df = df.fillna(0, subset=sensor_cols)

    print("5. Parquet으로 GCS 저장...")
    df.write \
        .mode('overwrite') \
        .parquet(f'gs://{BUCKET_NAME}/processed/sensor_data.parquet')

    print("6. 날짜별 집계...")
    daily_stats = df.groupBy('date', 'machine_status') \
        .agg(
            F.count('*').alias('count'),
            F.sum('is_anomaly').alias('anomaly_count'),
            (F.sum('is_anomaly') / F.count('*') * 100).alias('anomaly_rate'),
            F.sum(F.when(F.col('machine_status') == 'BROKEN', 1).otherwise(0)).alias('broken_count')
        ) \
        .orderBy('date')

    print("7. BigQuery에 저장...")
    from google.cloud import bigquery

    # 집계 결과를 GCS에 Parquet으로 저장
    daily_stats.write \
        .mode('overwrite') \
        .parquet(f'gs://{BUCKET_NAME}/processed/daily_stats.parquet')

    # GCS Parquet → BigQuery 로드
    bq_client = bigquery.Client(project=PROJECT_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    uri = f'gs://{BUCKET_NAME}/processed/daily_stats.parquet/*.parquet'
    table_ref = f'{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}'

    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()

    print(f"BigQuery 저장 완료: {table_ref}")
    return daily_stats

if __name__ == '__main__':
    spark = create_spark_session()
    result = process_sensor_data(spark)
    result.show(10)
    spark.stop()