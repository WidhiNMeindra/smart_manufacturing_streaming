import json
import logging
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

# Konfigurasi logging untuk output yang bersih
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def run_db_writer():
    """
    Fungsi utama untuk menjalankan consumer yang menulis data ke database.
    """
    try:
        # --- 1. Koneksi ke Kafka ---
        consumer = KafkaConsumer(
            "clean_sensor_data",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            group_id="db-writer-group-final",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        log.info("Berhasil terhubung ke Kafka sebagai consumer.")
    except Exception as e:
        log.error(f"Gagal terhubung ke Kafka: {e}")
        return

    try:
        # --- 2. Koneksi ke Database ---
        engine = create_engine(
            "postgresql+psycopg2://user:password@postgres:5432/machine_db"
        )
        conn = engine.connect()
        log.info("Berhasil terhubung ke database PostgreSQL.")
    except Exception as e:
        log.error(f"Gagal terhubung ke database: {e}")
        return

    # --- 3. Loop Utama untuk Membaca dari Kafka dan Menulis ke Database ---
    try:
        log.info("Mulai menulis data sensor ke database...")
        for message in consumer:
            data = message.value

            if "machine_id" not in data or data["machine_id"] is None:
                log.warning(f"Melewatkan data karena tidak ada machine_id: {data}")
                continue

            insert_query = text(
                """
                INSERT INTO sensor_readings (
                    event_timestamp, machine_id, vibration, acoustic,
                    temperature, current, status, label,
                    imf_1, imf_2, imf_3
                ) VALUES (
                    :event_timestamp, :machine_id, :vibration, :acoustic,
                    :temperature, :current, :status, :label,
                    :imf_1, :imf_2, :imf_3
                )
            """
            )
            
            with conn.begin():
                conn.execute(insert_query, data)

            log.info(
                f"Data untuk machine_id {data['machine_id']} berhasil disimpan."
            )

    except KeyboardInterrupt:
        log.warning("Proses dihentikan oleh pengguna.")
    except Exception as e:
        log.error("Terjadi error saat proses penulisan ke DB.", exc_info=True)
    finally:
        # --- 4. Menutup Koneksi ---
        if conn:
            conn.close()
            log.info("Koneksi database ditutup.")
        if consumer:
            consumer.close()
            log.info("Koneksi consumer Kafka ditutup.")


if __name__ == "__main__":
    log.info("Memulai script Database Writer...")
    run_db_writer()
