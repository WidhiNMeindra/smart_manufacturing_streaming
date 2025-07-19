# src/spark_consumer.py

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# Konfigurasi logging untuk output yang bersih
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def run_spark_consumer_basic():
    """
    Fungsi utama untuk menjalankan pekerjaan Spark Streaming dasar.
    Membaca data sensor mentah, mentransformasikannya, dan menulis kembali ke Kafka.
    """
    try:
        # --- 1. Membuat SparkSession ---
        # Menggunakan classpath eksplisit untuk memastikan driver Kafka dimuat dengan andal.
        spark = (
            SparkSession.builder.appName("BasicSparkStreamingConsumer")
            .master("spark://spark-master:7077")
            .config(
                "spark.driver.extraClassPath",
                "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar",
            )
            .config(
                "spark.executor.extraClassPath",
                "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar",
            )
            .getOrCreate()
        )
        log.info("SparkSession berhasil dibuat.")

        # --- 2. Mendefinisikan Skema Data ---
        # Skema ini harus cocok dengan data JSON yang dikirim oleh producer.
        raw_schema = StructType(
            [
                StructField("timestamp", StringType(), True),
                StructField("machine_id", StringType(), True),
                StructField("vibration", DoubleType(), True),
                StructField("acoustic", DoubleType(), True),
                StructField("temperature", DoubleType(), True),
                StructField("current", DoubleType(), True),
                StructField("imf_1", DoubleType(), True),
                StructField("imf_2", DoubleType(), True),
                StructField("imf_3", DoubleType(), True),
                StructField("label", IntegerType(), True),
            ]
        )

        # --- 3. Membaca Aliran Data dari Kafka ---
        raw_stream_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "raw_sensor_data_v2")
            .option("startingOffsets", "earliest")
            .load()
        )
        log.info("Berhasil terhubung ke aliran data Kafka 'raw_sensor_data_v2'.")

        # --- 4. Pipeline Transformasi ---
        # Parsing data JSON dari kolom 'value'
        parsed_df = raw_stream_df.select(
            from_json(col("value").cast("string"), raw_schema).alias("data")
        ).select("data.*")

        # Menerapkan transformasi dasar
        transformed_df = (
            parsed_df.withColumn(
                "event_timestamp", to_timestamp(col("timestamp"))
            )
            .withColumn("machine_id", col("machine_id").cast(IntegerType()))
            .withColumn(
                "status",
                when(col("label") == 1, "Failure Detected").otherwise("Normal"),
            )
        )

        # Memilih kolom akhir yang akan dikirim
        final_df = transformed_df.select(
            "event_timestamp",
            "machine_id",
            "vibration",
            "acoustic",
            "temperature",
            "current",
            "status",
            "label",
            "imf_1",
            "imf_2",
            "imf_3",
        )

        log.info("Pipeline transformasi telah dibangun.")

        # --- 5. Menulis Hasil Kembali ke Kafka ---
        # Mengemas kembali DataFrame menjadi satu kolom 'value' dalam format JSON
        kafka_output_df = final_df.select(expr("to_json(struct(*)) AS value"))

        # Menulis ke topik Kafka yang sudah bersih
        query = (
            kafka_output_df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("topic", "clean_sensor_data_v2") # Topik output untuk data bersih
            .option(
                "checkpointLocation", "/tmp/spark_checkpoints/basic_clean_data_writer"
            )
            .start()
        )

        log.info(
            "Pekerjaan streaming telah dimulai. Menulis data bersih ke topik 'clean_sensor_data_v2'."
        )

        # Menunggu query selesai (tidak akan pernah selesai untuk streaming)
        query.awaitTermination()

    except Exception as e:
        log.error("Terjadi error pada pekerjaan Spark Streaming.", exc_info=True)


if __name__ == "__main__":
    log.info("Memulai script Spark Consumer...")
    run_spark_consumer_basic()
