# Demo of an ETL Spark Job

Just some simple Spark code to be built using the demo infrastructure and process

## CDC

Implementation of a file based Change Data Capture flow. The flow can be run in local mode with default arguments running
 class `uk.co.pragmasoft.experiments.bigdata.spark.cdc.SampleCDC` or submitted into the spark cluster using the command line

 ```
 spark-submit
    --master <master>
    spark-experiments_2.10-1.0.jar
    --class uk.co.pragmasoft.experiments.bigdata.spark.cdc.SampleCDC
    <root path to CDC files>
 ```

Where `root path to CDC files` can be something like `file:/Users/stefano/projects/spark-experiments/data/cdc` if your submit to a local cluster
or an HDFS path if you submit to an HDFS-enabled installation.

The CDC files directory is assumed to contain two files: `fullData.csv` and `newData.csv`. It will generate the output into the `out` sub-folder

## DB Import

Application of the CDC flow with one of the source being an Oracle table.

The main class is `uk.co.pragmasoft.experiments.bigdata.spark.dbimport.CustomCDCDataBaseImporter` and is as parameters:

* `rootPath`: is the root path to CDC files, the program is expecting to find the `fullData.csv` file there. Defaults to `./data/cdc`
* `dbServerConnection` is Connection to the DB in the format `(user/pwd)@//host(:port)` and defaults to `"system/oracle@//192.168.59.103:49161`
* `dbName` is the SSID of the DB to connect to. Defaults to `xe`
* `local`: if specified causes the spark context to be built in local mode

### Testing the DB Import

To test the application you can follow the procedure documented here:

### Providing an Oracle Instance

The execution is assuming to have an Oracle instance available. I'm testing it using an oracle Docker instance. I'm using `alexeiled/docker-oracle-xe-11g:latest`.

You can install the instance with

```bash
docker pull alexeiled/docker-oracle-xe-11g
```

and launch it with

```bash
docker run -d -p 49160:22 -p 49161:1521 -p 49162:8080 
    -v /Users/stefano/projects/spark-experiments/data:/test/data 
    -v /Users/stefano/projects/spark-experiments/src/test/resources:/test/cfg 
    alexeiled/docker-oracle-xe-11g
```

You can use the script `src/test/scripts/start-db-docker.sh`

### Create the Customer Table 

The file `src/main/resources/OracleDB.sql` contains the definition of the Customer table and the generation of some sample data. 
Execute it with a SQL client connected with your DB.
I'm using SQLDeveloper connected with the configuration: 

* Server: 192.168.59.103 (I'm running Docker with boot2docker so you can get the ip with the command `boot2docker ip`)
* Port: 49161
* SSID: xe
* Username/pwd: system/oracle

You can also create the table using the command line:

```bash
docker exec -i -t <docker-id>  
    bash -c 'export ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe;export ORACLE_SID=XE;echo exit |  /u01/app/oracle/product/11.2.0/xe/bin/sqlplus system/oracle  @/test/cfg/OracleDB.sql'
```

You can use the script `src/test/scripts/init-db-docker.sh` and stop with `src/test/scripts/stop-db-docker.sh`

### Enabling Spark to Access Oracle

You need to add the oracle driver to your spark installation that means that every executor need to have the oracle driver `ojdbc6.jar` in their classpath.
If you are using a standalone cluster a good way to do that is to edit the `spark-defaults.conf` file adding the following two lines:

```
spark.executor.extraClassPath=/Users/stefano/projects/spark-experiments/extra-libs/ojdbc6.jar
spark.driver.extraClassPath=/Users/stefano/projects/spark-experiments/extra-libs/ojdbc6.jar
```
 
# Submit the job to Spark 
 
You can then submit the assembly jar to the cluster (in this example to a standalone cluster running on my laptop):

```bash
spark-submit 
    --master spark://galarragas.local:7077 
    --class uk.co.pragmasoft.experiments.bigdata.spark.dbimport.CustomerCDCDataBaseImporter 
    SparkExperiments-assembly-1.0.jar 
    --rootPath file:/Users/stefano/projects/spark-experiments/data/cdc
```
 
Remember to build the jar with `sbt assembly` and not with just `sbt package`.

This is my review test
