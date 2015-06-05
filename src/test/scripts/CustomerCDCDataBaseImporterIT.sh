#!/usr/bin/env bash

if [ -z ${1+x} ]; then
    ROOT_DIR=$PWD
else
    ROOT_DIR=$1
fi

echo "Root dir is $ROOT_DIR"

SPARK_HOME=~/opensource-projects/spark
SPARK_MASTER=spark://galarragas.local:7077
TEST_ROOT_PATH=$ROOT_DIR/data/cdc
TEST_ROOT_PATH_URL=file:$TEST_ROOT_PATH

RMDIR="rm -rf"
#RMDIR=hdfs dfs -rm -r

CAT=cat
#CAT=hadoop fs -cat

echo "Cleaning output folder"
`$RMDIR $TEST_ROOT_PATH/out/*`

echo "Submit CDC importer job"
$SPARK_HOME/bin/spark-submit \
    --master $SPARK_MASTER \
    --class uk.co.pragmasoft.experiments.bigdata.spark.dbimport.CustomerCDCDataBaseImporter \
    /Users/stefano/projects/spark-experiments/target/scala-2.10/SparkExperiments-assembly-1.0.jar \
    --rootPath $TEST_ROOT_PATH_URL


if [ $? -ne 0 ]; then
    echo "Spark command failed"
    exit 1
fi

echo "Check if there is any processed record with error"
$CAT $TEST_ROOT_PATH/out/errors.txt/part* > errors.txt
ERROR_COUNT=`wc -l errors.txt`

if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "Spark job generated error lines"
    cat errors.txt
    exit 1
fi

$CAT $TEST_ROOT_PATH/out/cdc.csv/part* | sort > out.csv

cat > expected-out.csv <<- EOM
10001,Stefano,New Home,U
10003,Tiago,Another address,I
10004,Antonios,home,I
10104,To be Deleted,Old Home,D
customerId,name,address,cdc
EOM

echo "Comparing output with expected out"
diff out.csv expected-out.csv

if [ $? -ne 0 ]; then
    echo "!!! Output of spark job different than expected, see output above for details"
    exit 1
fi

echo "Test completed successfully"

exit 0