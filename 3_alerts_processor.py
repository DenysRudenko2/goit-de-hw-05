from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json
from datetime import datetime

# Створення Kafka Consumer для читання даних з датчиків
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',  # Читаємо тільки нові повідомлення
    enable_auto_commit=True,
    group_id='alerts_processor_group'
)

# Створення Kafka Producer для відправки алертів
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назви топіків
my_name = "denys_rudenko"
sensors_topic = f'{my_name}_building_sensors'
temp_alerts_topic = f'{my_name}_temperature_alerts'
humidity_alerts_topic = f'{my_name}_humidity_alerts'

# Порогові значення
TEMP_THRESHOLD_HIGH = 40  # °C
HUMIDITY_THRESHOLD_HIGH = 80  # %
HUMIDITY_THRESHOLD_LOW = 20  # %

# Підписка на топік з даними датчиків
consumer.subscribe([sensors_topic])
print(f"Alert processor started. Monitoring topic: {sensors_topic}")
print(f"Temperature threshold: >{TEMP_THRESHOLD_HIGH}°C")
print(f"Humidity thresholds: <{HUMIDITY_THRESHOLD_LOW}% or >{HUMIDITY_THRESHOLD_HIGH}%")
print("-" * 60)

try:
    for message in consumer:
        sensor_data = message.value
        sensor_id = sensor_data.get('sensor_id')
        temperature = sensor_data.get('temperature')
        humidity = sensor_data.get('humidity')
        timestamp = sensor_data.get('timestamp')
        
        print(f"Received: {sensor_id} - Temp: {temperature}°C, Humidity: {humidity}%")
        
        # Перевірка температури
        if temperature > TEMP_THRESHOLD_HIGH:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "alert_time": datetime.now().isoformat(),
                "temperature": temperature,
                "threshold": TEMP_THRESHOLD_HIGH,
                "message": f"HIGH TEMPERATURE ALERT! Temperature {temperature}°C exceeds threshold {TEMP_THRESHOLD_HIGH}°C",
                "alert_type": "temperature_high"
            }
            producer.send(temp_alerts_topic, key=sensor_id, value=alert)
            print(f"  ⚠️  TEMPERATURE ALERT sent for {sensor_id}: {temperature}°C")
        
        # Перевірка вологості
        if humidity > HUMIDITY_THRESHOLD_HIGH:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "alert_time": datetime.now().isoformat(),
                "humidity": humidity,
                "threshold": HUMIDITY_THRESHOLD_HIGH,
                "message": f"HIGH HUMIDITY ALERT! Humidity {humidity}% exceeds threshold {HUMIDITY_THRESHOLD_HIGH}%",
                "alert_type": "humidity_high"
            }
            producer.send(humidity_alerts_topic, key=sensor_id, value=alert)
            print(f"  ⚠️  HIGH HUMIDITY ALERT sent for {sensor_id}: {humidity}%")
            
        elif humidity < HUMIDITY_THRESHOLD_LOW:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "alert_time": datetime.now().isoformat(),
                "humidity": humidity,
                "threshold": HUMIDITY_THRESHOLD_LOW,
                "message": f"LOW HUMIDITY ALERT! Humidity {humidity}% is below threshold {HUMIDITY_THRESHOLD_LOW}%",
                "alert_type": "humidity_low"
            }
            producer.send(humidity_alerts_topic, key=sensor_id, value=alert)
            print(f"  ⚠️  LOW HUMIDITY ALERT sent for {sensor_id}: {humidity}%")
        
        producer.flush()
        
except KeyboardInterrupt:
    print("\nAlert processor stopped.")
except Exception as e:
    print(f"Error occurred: {e}")
finally:
    consumer.close()
    producer.close()
    print("Connections closed.")