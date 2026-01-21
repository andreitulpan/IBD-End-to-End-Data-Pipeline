# We switch to 'bullseye' (Stable Debian) to ensure Java 17 is available
FROM python:3.10-slim-bullseye

# 1. Install OpenJDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# 2. Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# 3. Install Python dependencies
RUN pip install pyspark==3.5.0 influxdb-client pandas

# 4. Set working directory
WORKDIR /app

# 5. Copy your script into the image
COPY spark_processor_v2.py .
COPY cf_root_ca.pem .

# 6. Default command to run your script
CMD ["python", "-u", "spark_processor_v2.py"]