#!/bin/bash
echo "Starting Spark History Server..."
/opt/spark/sbin/start-history-server.sh

/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --host spark-master \
  --port 7077 \
  --webui-port 8080
