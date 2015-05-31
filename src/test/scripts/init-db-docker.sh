#!/usr/bin/env bash

ORACLE_DOCKER_ID=`cat oracle.docker.id`

docker exec -i -t $ORACLE_DOCKER_ID \
    bash -c 'export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe;export ORACLE_SID=XE;echo exit |  /u01/app/oracle/product/11.2.0/xe/bin/sqlplus system/oracle  @/test/cfg/InitCustomer.sql'


