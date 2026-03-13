import csv
import json
import time
from kafka import KafkaProducer

# Kafka Producer 설정
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# CSV 파일 읽어서 Kafka로 전송
def send_sensor_data(file_path, delay=1):
    print("Producer 시작...")
    
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # 메시지 만들기
            message = {
                'timestamp': row['timestamp'],
                'machine_status': row['machine_status'],
            }
            
            # sensor 값들 추가
            for key, value in row.items():
                if key.startswith('sensor_'):
                    message[key] = float(value) if value else None
            
            # Kafka로 전송
            producer.send('sensor-data', value=message)
            print(f"전송: {message['timestamp']} | {message['machine_status']}")
            
            # delay초 대기 (실시간 시뮬레이션)
            time.sleep(delay)
    
    producer.flush()
    print("전송 완료!")

if __name__ == '__main__':
    send_sensor_data('data/raw/sensor.csv', delay=0.1)