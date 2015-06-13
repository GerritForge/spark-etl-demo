#!/usr/bin/env bash

ORACLE_NAME=$1
ROOT_DIR=$2

echo "Starting CDH - Root dir is $ROOT_DIR"

docker run -d --link $ORACLE_NAME:$ORACLE_NAME -v $ROOT_DIR:/root/spark-etl mesos3.gerritforge.com:5000/cdh-juc > cdh.docker.id
