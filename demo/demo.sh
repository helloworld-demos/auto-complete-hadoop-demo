#!/bin/sh
set -e
set -x

sh ~/start-hadoop.sh

hdfs dfs -mkdir -p input
hdfs dfs -put data/*.txt input/
#TODO hdfs dfs -put plaintext_articles/*.txt input/

# not necessary
hdfs dfs -rm -r /output || true

# add mysql-connector
hdfs dfs -rm -r /mysql || true \
  && hdfs dfs -mkdir /mysql \
  && hdfs dfs -put mysql-connector-java-*.jar /mysql/

hadoop jar auto-complete-hadoop-demo.jar \
  -DnoGram=5 \
  -DinputPath=input \
  -DtempPath=/output \
  -Dthreshold=30 \
  -DtopK=7 \
  -DmysqlConnectorPath=/mysql/mysql-connector-java-5.1.39-bin.jar

# helper
# hdfs dfs -cat /output/*