#!/bin/bash
echo "Starting Spark Master..."
/opt/spark/sbin/start-master.sh
tail -f /opt/spark/logs/*
