# Mulai dari image dasar Jupyter
FROM jupyter/scipy-notebook:latest

# Ganti ke user root untuk mendapatkan izin instalasi
USER root

# 1. Update daftar paket
RUN apt-get update && \
# 2. Instal OpenJDK versi 11 tanpa meminta konfirmasi
    apt-get install -y openjdk-11-jdk && \
# 3. Membersihkan cache untuk menjaga ukuran image tetap kecil
    apt-get clean

# 4. Set environment variable JAVA_HOME agar Spark bisa menemukannya
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Instal versi PySpark yang kita inginkan
RUN pip install pyspark==3.3.0

# Ganti kembali ke user default 'jovyan' untuk keamanan
USER jovyan
