#!/bin/bash
until getent hosts spark-master; do
    echo "Waiting for spark-master..."
    sleep 2
done

/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
  spark://spark-master:7077 \
  --webui-port 8081
