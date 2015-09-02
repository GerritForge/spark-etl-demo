#!/usr/bin/env bash

set -e

if [ -z ${1+x} ]; then
    ROOT_DIR=$PWD
else
    ROOT_DIR=$1
fi

set +x
echo "Waiting for Spark to startup"
waitCount=0
while ! echo exit | nc $HOSTNAME 7077; do
    waitCount=$((waitCount + 1))

    if [ $waitCount -gt 40 ]; then
        echo "Spark instance didn't startup properly"
        exit -1
    fi

    sleep 5
    echo "."
done
echo "Ready"
set -x


ORACLE_DB=$2
echo $ORACLE_DB
echo "Root dir is $ROOT_DIR"

SPARK_HOME=/usr/lib/spark
SPARK_MASTER=spark://$(hostname):7077
TEST_ROOT_PATH=$ROOT_DIR/data/cdc
TEST_ROOT_PATH_URL=hdfs://$TEST_ROOT_PATH
JAR_LOCATION=$ROOT_DIR/target/scala-2.10/SparkExperiments-assembly-1.0.jar

#RMDIR="rm -rf"
RMDIR="hdfs dfs -rm -r"

#CAT=cat
CAT="hadoop fs -cat"

(hadoop fs -test -d $ROOT_DIR/data/out && $RMDIR $ROOT_DIR/data/out) || true

hadoop fs -mkdir -p $ROOT_DIR/data

echo "Copying data to HDFS"
HADOOP_CMD="hadoop fs -copyFromLocal $ROOT_DIR/data $ROOT_DIR"
echo $HADOOP_CMD
`$HADOOP_CMD`

echo "Content of HDFS data directory"
hdfs dfs -ls $ROOT_DIR/data

echo "Submit CDC importer job"
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class uk.co.pragmasoft.experiments.bigdata.spark.dbimport.CustomerCDCDataBaseImporter \
    $JAR_LOCATION \
    --rootPath $TEST_ROOT_PATH_URL \
    --dbServerConnection "system/oracle@$ORACLE_DB:1521"


if [ $? -ne 0 ]; then
    echo "Spark command failed"
    exit 1
fi

echo "Check if there is any processed record with error"
$CAT $TEST_ROOT_PATH_URL/out/errors.txt/part* > errors.txt
ERROR_COUNT=`cat errors.txt | wc -l`

if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "Spark job generated error lines"
    cat errors.txt
    exit 1
fi

`$CAT $TEST_ROOT_PATH_URL/out/cdc.csv/part* | sort > out.csv`

cat > expected-out.csv <<- EOM
10001,Stefano,New Home,U
10003,Tiago,Another address,I
10004,Antonios,home,I
10104,To be Deleted,Old Home,D
customerId,name,address,cdc
EOM

echo "problem fixed"

echo "Comparing output with expected out"

diff out.csv expected-out.csv

if [ $? -ne 0 ]; then
    echo "!!! Output of spark job different than expected, see output above for details !!!"
    exit 1
fi

echo "Test completed successfully"

exit 0


THIS IS MY OTHER EXTRA LINE
