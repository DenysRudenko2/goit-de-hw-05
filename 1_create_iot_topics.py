from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення топіків з унікальними іменами
my_name = "denys_rudenko"
topics = [
    NewTopic(name=f'{my_name}_building_sensors', num_partitions=3, replication_factor=1),
    NewTopic(name=f'{my_name}_temperature_alerts', num_partitions=2, replication_factor=1),
    NewTopic(name=f'{my_name}_humidity_alerts', num_partitions=2, replication_factor=1)
]

# Створення топіків
for topic in topics:
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic.name}' created successfully.")
    except Exception as e:
        print(f"Topic '{topic.name}' creation failed: {e}")

# Перевіряємо список топіків
print("\nExisting topics containing your name:")
all_topics = admin_client.list_topics()
user_topics = [t for t in all_topics if my_name in t]
for topic in user_topics:
    print(f"  - {topic}")

admin_client.close()