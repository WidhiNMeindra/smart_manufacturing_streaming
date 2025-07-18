# Platform Data Streaming untuk Pemantauan Kondisi Mesin

Selamat datang di repositori proyek akhir Bootcamp Data Engineering! 
Proyek ini mendemonstrasikan pembangunan platform data end-to-end yang mampu menelan, memproses, dan memvisualisasikan data sensor mesin secara near real-time untuk tujuan pemantauan proaktif dan deteksi dini kegagalan.

## Teknologi yang Digunakan
- Containerization: Docker & Docker Compose
- Workflow Automation: Makefile
- Message Broker: Apache Kafka
- Data Processing: Apache Spark (Structured Streaming) & PySpark
- Database / Data Warehouse: PostgreSQL
- BI & Visualization: Metabase
- Programming Language: Python
- Core Libraries: Pandas, SQLAlchemy, Kafka-Python

## Fitur Utama
- Pipeline Data End-to-End: Mengotomatisasi seluruh alur data dari sumber hingga visualisasi.
- Pemrosesan Near Real-time: Data diproses dalam hitungan detik menggunakan Spark Structured Streaming.
- Pemantauan Proaktif: Dashboard live di Metabase untuk memantau metrik kunci seperti suhu, getaran, dan status mesin.
- Lingkungan yang Dapat Direproduksi: Seluruh infrastruktur didefinisikan dalam Docker Compose dan dikelola oleh Makefile, memastikan kemudahan setup.

## Struktur Proyek
```
final-project-streaming/
├── docker-compose.yml     # Mendefinisikan semua layanan (Kafka, Spark, dll.)
├── Dockerfile             # Membuat image Jupyter kustom dengan dependensi yang benar
├── Makefile               # Perintah singkat untuk mengelola lingkungan Docker
└── workspace/
    ├── data/
    │   └── predictive_maintenance_dataset.csv  # File dataset mentah
    ├── 1_kafka_producer.ipynb
    ├── 2_spark_streaming_consumer.ipynb
    ├── 3_create_database_table.ipynb
    ├── 4_database_writer_consumer.ipynb
    └── 5_alerting_consumer.ipynb
```

## Cara Menjalankan Proyek
*Prasyarat*
- Docker Desktop terinstal dan berjalan.
- Git terinstal.

*Instalasi & Setup*
1. Clone repositori ini:
- git clone https://www.andarepository.com/
- cd [nama-folder-repositori]
2. Tempatkan Dataset: Unduh dataset yang relevan dan letakkan file CSV di dalam folder workspace/data/.

*Menjalankan Pipeline*

Proyek ini dirancang untuk dijalankan langkah demi langkah menggunakan Makefile. Buka terminal di direktori utama proyek dan jalankan perintah berikut secara berurutan:

1. Jalankan semua layanan infrastruktur:
make all

2. Tunggu beberapa saat hingga semua kontainer stabil.
Buka Jupyter Notebook:
make jupyter

3. Ini akan membuka Jupyter di browser Anda.
Jalankan Notebook secara Berurutan:
- Buka dan jalankan 1_kafka_producer.ipynb untuk memulai aliran data.
- Buka dan jalankan 2_spark_streaming_consumer.ipynb untuk memproses data.
- Buka dan jalankan 3_create_database_table.ipynb untuk menyiapkan database.
- Buka dan jalankan 4_database_writer_consumer.ipynb untuk menyimpan data ke database.

4. Akses Metabase:
make metabase

*Lakukan setup awal dan hubungkan Metabase ke database PostgreSQL Anda menggunakan detail koneksi yang ada di docker-compose.yml.*


    
