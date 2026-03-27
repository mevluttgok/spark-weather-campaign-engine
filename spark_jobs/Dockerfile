FROM apache/spark

USER root

# Python bağımlılıkları
RUN pip install --default-timeout=1000 --no-cache-dir \
    pyspark==3.5.0 \
    kafka-python==2.0.2 \
    python-dotenv==1.0.1

# Job dosyaları
WORKDIR /opt/spark_jobs
COPY . .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/bin/bash", "entrypoint.sh"]
