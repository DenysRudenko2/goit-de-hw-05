from kafka import KafkaConsumer
from configs import kafka_config
import json
from datetime import datetime

# Створення Kafka Consumer для читання алертів
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='latest',  # Читаємо тільки нові алерти
    enable_auto_commit=True,
    group_id='alerts_reader_group'
)

# Назви топіків
my_name = "denys_rudenko"
temp_alerts_topic = f'{my_name}_temperature_alerts'
humidity_alerts_topic = f'{my_name}_humidity_alerts'

# Підписка на обидва топіки з алертами
consumer.subscribe([temp_alerts_topic, humidity_alerts_topic])

print("=" * 80)
print("🚨 ALERT MONITORING SYSTEM STARTED 🚨")
print("=" * 80)
print(f"Monitoring topics:")
print(f"  - {temp_alerts_topic}")
print(f"  - {humidity_alerts_topic}")
print("-" * 80)

# Лічильники алертів
alert_counts = {
    'temperature': 0,
    'humidity_high': 0,
    'humidity_low': 0
}

try:
    print("Waiting for alerts...\n")
    
    for message in consumer:
        alert_data = message.value
        topic = message.topic
        
        # Визначення типу алерту
        if 'temperature' in topic:
            alert_type = "🌡️  TEMPERATURE"
            alert_counts['temperature'] += 1
            value = alert_data.get('temperature')
            unit = "°C"
        else:  # humidity alerts
            if alert_data.get('alert_type') == 'humidity_high':
                alert_type = "💧 HIGH HUMIDITY"
                alert_counts['humidity_high'] += 1
            else:
                alert_type = "🏜️  LOW HUMIDITY"
                alert_counts['humidity_low'] += 1
            value = alert_data.get('humidity')
            unit = "%"
        
        # Форматований вивід алерту
        print("=" * 80)
        print(f"{alert_type} ALERT!")
        print("-" * 80)
        print(f"📍 Sensor ID:    {alert_data.get('sensor_id')}")
        print(f"📊 Value:        {value}{unit}")
        print(f"⚠️  Threshold:    {alert_data.get('threshold')}{unit}")
        print(f"🕐 Sensor Time:  {alert_data.get('timestamp')}")
        print(f"🕑 Alert Time:   {alert_data.get('alert_time')}")
        print(f"📝 Message:      {alert_data.get('message')}")
        print("-" * 80)
        print(f"📈 Total Alerts - Temp: {alert_counts['temperature']}, "
              f"High Humidity: {alert_counts['humidity_high']}, "
              f"Low Humidity: {alert_counts['humidity_low']}")
        print("=" * 80)
        print()
        
except KeyboardInterrupt:
    print("\n" + "=" * 80)
    print("Alert monitoring stopped.")
    print(f"Final statistics:")
    print(f"  - Temperature alerts:    {alert_counts['temperature']}")
    print(f"  - High humidity alerts:  {alert_counts['humidity_high']}")
    print(f"  - Low humidity alerts:   {alert_counts['humidity_low']}")
    print(f"  - Total alerts:          {sum(alert_counts.values())}")
    print("=" * 80)
except Exception as e:
    print(f"Error occurred: {e}")
finally:
    consumer.close()
    print("Consumer connection closed.")