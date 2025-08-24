# IoT Monitoring System with Kafka

## Опис проекту
Система моніторингу IoT датчиків температури та вологості з використанням Apache Kafka для обробки потокових даних.

## Архітектура системи

```
Sensor Simulators → building_sensors → Alert Processor → temperature_alerts → Alert Reader
                                                      ↘ humidity_alerts    ↗
```

## Компоненти

### 1. Створення топіків (`1_create_iot_topics.py`)
Створює три топіки в Kafka:
- `denys_rudenko_building_sensors` - для даних з датчиків (3 партиції)
- `denys_rudenko_temperature_alerts` - для температурних алертів (2 партиції)
- `denys_rudenko_humidity_alerts` - для алертів вологості (2 партиції)

### 2. Симулятор датчика (`2_sensor_simulator.py`)
- Генерує унікальний ID датчика для кожної сесії
- Відправляє випадкові дані кожні 2-5 секунд:
  - Температура: 25-45°C
  - Вологість: 15-85%
- Можна запустити декілька екземплярів для симуляції різних датчиків

### 3. Процесор алертів (`3_alerts_processor.py`)
- Читає дані з `building_sensors`
- Генерує алерти при:
  - Температура > 40°C → `temperature_alerts`
  - Вологість > 80% або < 20% → `humidity_alerts`
- Алерти містять: sensor_id, значення, час, повідомлення

### 4. Читач алертів (`4_alerts_reader.py`)
- Підписується на обидва топіки алертів
- Виводить форматовані повідомлення про алерти
- Веде статистику алертів

## Запуск системи

### Крок 1: Створення топіків
```bash
python 1_create_iot_topics.py
```

### Крок 2: Запуск процесора алертів
```bash
python 3_alerts_processor.py
```

### Крок 3: Запуск читача алертів (в новому терміналі)
```bash
python 4_alerts_reader.py
```

### Крок 4: Запуск симуляторів датчиків (в нових терміналах)
```bash
# Датчик 1
python 2_sensor_simulator.py

# Датчик 2 (в новому терміналі)
python 2_sensor_simulator.py

# Можна запустити скільки завгодно датчиків
```

## Моніторинг через PyCharm Kafka Plugin

1. Відкрийте вкладку Big Data Tools
2. Перейдіть до Topics
3. Знайдіть топіки:
   - `denys_rudenko_building_sensors`
   - `denys_rudenko_temperature_alerts`
   - `denys_rudenko_humidity_alerts`
4. Можна переглядати повідомлення в реальному часі

## Приклад виводу

### Sensor Simulator:
```
Starting sensor simulation with ID: sensor_4521
[1] Sent: Temp=32.5°C, Humidity=65.3%
[2] Sent: Temp=41.2°C, Humidity=82.1%
```

### Alerts Processor:
```
Received: sensor_4521 - Temp: 41.2°C, Humidity: 82.1%
  ⚠️  TEMPERATURE ALERT sent for sensor_4521: 41.2°C
  ⚠️  HIGH HUMIDITY ALERT sent for sensor_4521: 82.1%
```

### Alerts Reader:
```
================================================================================
🌡️  TEMPERATURE ALERT!
--------------------------------------------------------------------------------
📍 Sensor ID:    sensor_4521
📊 Value:        41.2°C
⚠️  Threshold:    40°C
📝 Message:      HIGH TEMPERATURE ALERT! Temperature 41.2°C exceeds threshold 40°C
================================================================================
```

## Зупинка системи
Натисніть `Ctrl+C` в кожному терміналі для зупинки відповідного компонента.