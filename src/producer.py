import pandas as pd
from kafka import KafkaProducer
import json
import time
import logging

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def run_producer():
    """Fungsi utama untuk menjalankan Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:29092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        log.info("Kafka Producer berhasil terhubung.")
    except Exception as e:
        log.error(f"Gagal terhubung ke Kafka: {e}")
        return # Hentikan eksekusi jika tidak bisa terhubung

    file_path = 'workspace/data/predictive_maintenance_dataset.csv'
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.lower()
        log.info(f"Dataset '{file_path}' berhasil dimuat.")
        
        KAFKA_TOPIC = 'raw_sensor_data_'
        for index, row in df.iterrows():
            message = row.to_dict()
            producer.send(KAFKA_TOPIC, value=message)
            machine_id = message.get('machine_id', 'N/A')
            event_time = message.get('timestamp', 'N/A')
            log.info(f"Mengirim data untuk machine_id {machine_id} pada waktu {event_time}")
            time.sleep(1)
            
        producer.flush()
        log.info("Semua data berhasil dikirim.")
        
    except Exception as e:
        log.error(f"Terjadi error saat memproses data: {e}", exc_info=True)

if __name__ == "__main__":
    log.info("Memulai script producer...")
    run_producer()
    log.info("Script producer selesai.")