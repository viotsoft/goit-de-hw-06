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

def generate_sensor_data(sensor_id):
    """Генерація даних сенсора"""
    return {
        "sensor_id": sensor_id,
        "timestamp": str(int(time.time())),
        "temperature": random.randint(20, 35),  # Температура між 20-35
        "humidity": random.randint(30, 60)      # Вологість між 30-60%
    }

def main():
    print("🚀 Starting sensor data generator...")
    
    try:
        producer = create_producer()
        print("✅ Connected to Kafka successfully")
        
        topic = "building_sensors_greenmoon"
        sensor_count = 5  # Кількість сенсорів
        messages_per_sensor = 10  # Повідомлень на сенсор
        
        total_messages = sensor_count * messages_per_sensor
        print(f"📊 Generating {total_messages} messages from {sensor_count} sensors...")
        
        for sensor_id in range(1, sensor_count + 1):
            print(f"📡 Sensor {sensor_id}: Generating {messages_per_sensor} messages...")
            
            for i in range(messages_per_sensor):
                data = generate_sensor_data(sensor_id)
                
                # Відправка повідомлення
                producer.send(topic, value=data)
                
                print(f"  Message {i+1}: temp={data['temperature']}°C, humidity={data['humidity']}%")
                
                # Невелика затримка між повідомленнями
                time.sleep(0.5)
            
            print(f"✅ Sensor {sensor_id} completed")
        
        # Чекаємо на відправку всіх повідомлень
        producer.flush()
        print(f"🎉 All {total_messages} messages sent successfully to topic '{topic}'")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
