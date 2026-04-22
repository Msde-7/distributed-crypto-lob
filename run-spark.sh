#!/bin/bash
# Launcher for the Spark streaming job on Windows (Git Bash).
# Run from the projectfil directory: ./run-spark.sh

export JAVA_HOME="C:\\Program Files\\Microsoft\\jdk-17.0.18.8-hotspot"
export SPARK_HOME="C:\\tools\\spark-3.5.8-bin-hadoop3"
export HADOOP_HOME="C:\\tools\\hadoop"

cmd.exe //c "set JAVA_HOME=${JAVA_HOME}&& set SPARK_HOME=${SPARK_HOME}&& set HADOOP_HOME=${HADOOP_HOME}&& set PATH=%JAVA_HOME%\\bin;%SPARK_HOME%\\bin;%HADOOP_HOME%\\bin;%PATH%&& spark-submit.cmd --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_order_book.py"
