from kafka import KafkaProducer
from configs import kafka_config
import json
import time
import random
import sys
from datetime import datetime

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Генерація унікального ID для датчика (постійний для сесії)
sensor_id = f"sensor_{random.randint(1000, 9999)}"
print(f"Starting sensor simulation with ID: {sensor_id}")

# Назва топіку
my_name = "denys_rudenko"
topic_name = f'{my_name}_building_sensors'

try:
    message_count = 0
    while True:
        # Генерація випадкових даних датчика
        sensor_data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().isoformat(),
            "temperature": round(random.uniform(25, 45), 1),  # Температура від 25 до 45°C
            "humidity": round(random.uniform(15, 85), 1)      # Вологість від 15 до 85%
        }
        
        # Відправлення даних в топік
        producer.send(topic_name, key=sensor_id, value=sensor_data)
        producer.flush()
        
        message_count += 1
        print(f"[{message_count}] Sent: Temp={sensor_data['temperature']}°C, "
              f"Humidity={sensor_data['humidity']}%")
        
        # Затримка між вимірюваннями (2-5 секунд)
        time.sleep(random.uniform(2, 5))
        
except KeyboardInterrupt:
    print(f"\nSensor {sensor_id} stopped. Total messages sent: {message_count}")
except Exception as e:
    print(f"Error occurred: {e}")
finally:
    producer.close()
    print("Producer connection closed.")