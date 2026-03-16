from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
import json

def detect_anomaly(data: str) -> str:
    """센서 데이터에서 이상 감지"""
    record = json.loads(data)
    
    status = record.get('machine_status', '')
    timestamp = record.get('timestamp', '')
    
    # BROKEN 감지 시 경보
    if status == 'BROKEN':
        return f"[경보] BROKEN 감지! timestamp: {timestamp}"
    
    return None

def main():
    # 실행 환경 설정
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # JAR 파일 경로 추가
    env.add_jars("file:///C:/mes-sensor-pipeline/pipeline/flink/flink-sql-connector-kafka-4.0.0-2.0.jar")

    # Kafka Source 설정
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_topics('sensor-data') \
        .set_group_id('flink-sensor-group') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 데이터 스트림 생성
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        'Kafka Source'
    )

    # 이상 감지 적용
    alerts = stream \
        .map(detect_anomaly) \
        .filter(lambda x: x is not None)

    # 결과 출력
    alerts.print()

    # 실행
    env.execute('Sensor Anomaly Detection')

if __name__ == '__main__':
    main()