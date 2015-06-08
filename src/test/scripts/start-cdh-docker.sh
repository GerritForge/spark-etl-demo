#!/usr/bin/env bash

ORACLE_NAME=$1
ROOT_DIR=$2

echo "Starting CDH - Root dir is $ROOT_DIR"

SPARK_JAR_PATH=$ROOT_DIR/target/scala-2.10/

docker run -d --link $ORACLE_NAME:$ORACLE_NAME -v $SPARK_JAR_PATH:/root cdh-juc > cdh.docker.id
