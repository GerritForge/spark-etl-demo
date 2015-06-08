#!/usr/bin/env bash

if [ -z ${1+x} ]; then
    ROOT_DIR=$PWD
else
    ROOT_DIR=$1
fi

ORACLE_NAME=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1`

## START_ORACLE
./start-db-docker.sh $ORACLE_NAME $ROOT_DIR

# Check if oracle instance started
ORACLE_DOCKER_ID=`cat oracle.docker.id`
ORACLE_STARTED=0

while [ $ORACLE_STARTED == 0 ]
do
  ORACLE_START=`docker logs $ORACLE_DOCKER_ID | grep 'ORACLE STARTED'`
 
  if [ "$ORACLE_START" != "" ]; then
	echo "ORACLE STARTED"
	ORACLE_STARTED=1
	./init-db-docker.sh
  fi
sleep 10
done

# START CDH
./start-cdh-docker.sh $ORACLE_NAME $ROOT_DIR

# Check if CDH instance started
CDH_DOCKER_ID=`cat cdh.docker.id`
CDH_STARTED=0
echo $CDH_DOCKER_ID
while [ $CDH_STARTED == 0 ]
do
  CDH_START=`docker logs $CDH_DOCKER_ID | grep 'CDH STARTED'`

  if [ "$CDH_START" != "" ]; then
        echo "CDH STARTED"
        CDH_STARTED=1
  fi
sleep 5
done


COMMAND_EXEC="export SPARK_CLASSPATH=/root/spark-etl/extra-libs/ojdbc6.jar; ./root/spark-etl/src/test/scripts/CustomerCDCDataBaseImporterIT.sh /root/spark-etl $ORACLE_NAME"

echo "Executing the Spark JOB"
docker exec -i -t $CDH_DOCKER_ID \
    bash -c "$COMMAND_EXEC"

