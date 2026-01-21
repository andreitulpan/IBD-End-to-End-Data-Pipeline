FROM python:3.10-slim-bullseye

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install pyspark==3.5.0 influxdb-client pandas

WORKDIR /app

COPY spark_processor_v2.py .
COPY cf_root_ca.pem .

CMD ["python", "-u", "spark_processor_v2.py"]
