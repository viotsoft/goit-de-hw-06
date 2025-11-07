from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import signal
import sys

print("=== Improved Kafka Spark Streaming ===")

# Глобальна змінна для управління потоком
is_running = True

def signal_handler(sig, frame):
    global is_running
    print("\n🛑 Received interrupt signal. Stopping gracefully...")
    is_running = False

# Реєструємо обробник сигналів
signal.signal(signal.SIGINT, signal_handler)

spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(f"✅ Spark {spark.version} started")

try:
    # Завантажуємо умови алертів
    alerts_df = spark.read.csv("data/alerts_conditions.csv", header=True, inferSchema=True)
    print("📋 Alerts conditions loaded")
    alerts_df.show()
    
    # Створюємо потік з Kafka
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 
                'org.apache.kafka.common.security.plain.PlainLoginModule required '
                'username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
        .option("subscribe", "building_sensors_greenmoon") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "10") \
        .load()
    
    print("🔌 Connected to Kafka successfully")
    
    # Схема для JSON даних
    json_schema = StructType([
        StructField("sensor_id", IntegerType()),
        StructField("timestamp", StringType()),
        StructField("temperature", IntegerType()),
        StructField("humidity", IntegerType())
    ])
    
    # Парсимо JSON
    parsed_stream = kafka_stream.select(
        from_json(col("value").cast("string"), json_schema).alias("data")
    ).select(
        col("data.sensor_id"),
        col("data.timestamp"),
        col("data.temperature"),
        col("data.humidity")
    )
    
    # Агрегації по вікну
    windowed_avg = parsed_stream \
        .withColumn("ts", from_unixtime(col("timestamp").cast("double")).cast(TimestampType())) \
        .withWatermark("ts", "30 seconds") \
        .groupBy(
            window(col("ts"), "1 minute", "30 seconds")
        ) \
        .agg(
            avg("temperature").alias("avg_temp"),
            avg("humidity").alias("avg_humidity"),
            count("*").alias("message_count")
        )
    
    # Перевірка алертів
    alerts = windowed_avg.crossJoin(alerts_df) \
        .filter(
            (col("avg_temp").between(col("temperature_min"), col("temperature_max"))) |
            (col("avg_humidity").between(col("humidity_min"), col("humidity_max")))
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_temp"),
            col("avg_humidity"),
            col("message_count"),
            col("code"),
            col("message"),
            current_timestamp().alias("alert_timestamp")
        )
    
    print("🚀 Starting streaming queries...")
    
    # Запускаємо потік для виводу в консоль
    console_query = alerts \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", 10) \
        .option("checkpointLocation", "/tmp/spark-kafka-checkpoint-console") \
        .start()
    
    # Підготовка даних для запису в Kafka
    kafka_output = alerts.select(
        col("code").alias("key"),
        to_json(
            struct(
                col("window_start"),
                col("window_end"), 
                col("avg_temp"),
                col("avg_humidity"),
                col("message_count"),
                col("code"),
                col("message"),
                col("alert_timestamp")
            )
        ).alias("value")
    )
    
    # Запис в Kafka
    kafka_query = kafka_output \
        .writeStream \
        .outputMode("update") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 
                'org.apache.kafka.common.security.plain.PlainLoginModule required '
                'username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
        .option("topic", "building_alerts_greenmoon") \
        .option("checkpointLocation", "/tmp/kafka-output-checkpoint") \
        .start()
    
    print("✅ Both streams started successfully!")
    print("📊 Console output and Kafka writing active")
    print("📨 Alerts being written to: building_alerts_greenmoon")
    print("💡 Make sure generator.py is running in another terminal")
    print("⏹️  Press Ctrl+C to stop")
    
    # Чекаємо поки потік активний і ми не отримали сигнал зупинки
    while is_running and (console_query.isActive or kafka_query.isActive):
        time.sleep(1)
        status = console_query.status
    
    print("🛑 Stopping streams...")
    console_query.stop()
    kafka_query.stop()
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("✅ Spark session stopped cleanly")
