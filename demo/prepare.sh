#!/bin/sh
set -e
set -x

# shared folder between docker and os
SHARED_FOLDER=~/src
WORK_DIR=${SHARED_FOLDER}/auto-complete-demo
JOWAY_DOCKER_PATH=~/dev/hadoop-cluster-docker

# generate all necessary files inside docker container
rm -rf ${SHARED_FOLDER} || true && mkdir -p ${WORK_DIR}
(cd .. && mvn clean package -DskipTests && cp target/*.jar ${WORK_DIR}/auto-complete-hadoop-demo.jar)
cp mysql-connector-java-*.jar ${WORK_DIR}
cp -R data ${WORK_DIR}
#TODO cp -R plaintext_articles ${WORK_DIR}
cp demo.sh ${WORK_DIR}

# recreate database
(cd .. && mvn clean compile flyway:clean flyway:migrate flyway:info -Dflyway.configFile=flyway.properties)

# delete all hadoop containers
docker rm -f hadoop-slave1 hadoop-slave2 hadoop-master
