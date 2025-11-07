from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Конфігурація підключення до Kafka
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

def create_producer():
    """Створення Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_sensor_data(sensor_id, trigger_alerts=False):
    """Генерація даних сенсора"""
    if trigger_alerts:
        # Генеруємо дані, які викликають алерти
        # TEMP_HIGH: temperature_min=25,temperature_max=35 → генеруємо 26-34
        # TEMP_LOW: temperature_min=15,temperature_max=25 → генеруємо 16-24  
        # HUMIDITY_HIGH: humidity_min=40,humidity_max=50 → генеруємо 41-49
        # HUMIDITY_LOW: humidity_min=20,humidity_max=30 → генеруємо 21-29
        
        temperature = random.choice([18, 22, 28, 32])  # Значення що викликають алерти
        humidity = random.choice([25, 35, 45, 48])     # Значення що викликають алерти
    else:
        temperature = random.randint(20, 35)
        humidity = random.randint(30, 60)
    
    return {
        "sensor_id": sensor_id,
        "timestamp": str(int(time.time())),
        "temperature": temperature,
        "humidity": humidity
    }

def main():
    print("🚀 Starting sensor data generator with ALERT triggers...")
    
    try:
        producer = create_producer()
        print("✅ Connected to Kafka successfully")
        
        topic = "building_sensors_greenmoon"
        sensor_count = 3
        messages_per_sensor = 20
        
        total_messages = sensor_count * messages_per_sensor
        print(f"📊 Generating {total_messages} messages from {sensor_count} sensors...")
        print("💡 Generating data that will trigger alerts...")
        
        for sensor_id in range(1, sensor_count + 1):
            print(f"📡 Sensor {sensor_id}: Generating {messages_per_sensor} messages...")
            
            for i in range(messages_per_sensor):
                # Кожне 3-тє повідомлення генерує дані для алертів
                trigger_alert = (i % 3 == 0)
                data = generate_sensor_data(sensor_id, trigger_alert)
                
                producer.send(topic, value=data)
                
                alert_indicator = "🚨" if trigger_alert else "  "
                print(f"  {alert_indicator} Message {i+1}: temp={data['temperature']}°C, humidity={data['humidity']}%")
                
                time.sleep(1)  # 1 секунда між повідомленнями
            
            print(f"✅ Sensor {sensor_id} completed")
        
        producer.flush()
        print(f"🎉 All {total_messages} messages sent successfully!")
        print("🔔 Alerts should appear in building_alerts_greenmoon topic")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
