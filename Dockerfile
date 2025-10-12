# docker/Dockerfile
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

# Install openjdk for PySpark
RUN apt-get update && \
    apt-get install -y default-jdk curl build-essential && \
    rm -rf /var/lib/apt/lists/*

# Install pyspark and other pip deps
RUN pip install pyspark==3.5.1
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/app
COPY src /opt/app/src
ENV PYTHONPATH=/opt/app/src

ENTRYPOINT ["python", "/opt/app/src/spark_transform.py"]