#!/usr/bin/env python3
"""
Скрипт для перевірки створених топіків згідно з вимогами домашнього завдання
"""

from kafka.admin import KafkaAdminClient
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Ваше ім'я для фільтрації топіків
my_name = "denys_rudenko"

print("=" * 60)
print("Перевірка топіків для домашнього завдання")
print("=" * 60)

# Отримання та вивід топіків з вашим ім'ям
print(f"\nТопіки, що містять '{my_name}':")
print("-" * 60)

# Команда згідно з вимогами домашнього завдання
[print(topic) for topic in admin_client.list_topics() if my_name in topic]

print("-" * 60)

# Детальна інформація про топіки
print("\nПеревірка необхідних топіків:")
required_topics = [
    f"{my_name}_building_sensors",
    f"{my_name}_temperature_alerts", 
    f"{my_name}_humidity_alerts"
]

all_topics = admin_client.list_topics()
for topic in required_topics:
    if topic in all_topics:
        print(f"✅ {topic} - існує")
    else:
        print(f"❌ {topic} - НЕ ЗНАЙДЕНО")

print("=" * 60)

# Закриття з'єднання
admin_client.close()