#!/usr/bin/env bash

ORACLE_NAME=$1
ROOT_DIR=$2

echo "Root dir is $ROOT_DIR"

docker run -d -v $ROOT_DIR/data:/test/data -v $ROOT_DIR/src/test/resources:/test/cfg --name $ORACLE_NAME oracle-juc > oracle.docker.id
