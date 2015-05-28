package uk.co.pragmasoft.experiments.bigdata.spark.dbimport


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser
import uk.co.pragmasoft.experiments.bigdata.spark.cdc.Cdc._
import uk.co.pragmasoft.experiments.bigdata.spark.{CSVOperations, CustomerData}
import uk.co.pragmasoft.experiments.bigdata.spark.cdc.CdcSupport

object AppParams {
  case class Config(
                     isLocal: Boolean = false,
                     fileRootPath: String = "data/cdc", 
                     dbServerConnection: String = "system/oracle@//192.168.59.103:49161",
                     dbName: String = "xe"
                   )

  val optionParser = new OptionParser[Config]("CDC DB Import") {
    opt[String]("rootPath") action { (arg, config) =>
      config.copy(fileRootPath = arg)
    } text "Local file path"
    opt[String]("dbServerConnection") action { (arg, config) =>
      config.copy(dbServerConnection = arg)
    } text "Connection to the DB: format (user/pwd)@//host(:port)"
    opt[String]("dbName") action { (arg, config) =>
      config.copy(dbName = arg)
    } text "SSID of the DB to connect to"
    opt[Unit]("local") action { (arg, config) =>
      config.copy(isLocal = true)
    } text "run in local mode"
  }
}


object CustomerCDCDataBaseImporter extends App with CdcSupport {
  import CSVOperations._
  import AppParams._

  val parsedArgsMaybe = optionParser.parse(args, Config())
  
  require(parsedArgsMaybe.isDefined, "Invalid arguments")
  
  val parsedArgs = parsedArgsMaybe.get
  
  import parsedArgs._
  
  
  def filePath(file: String) = s"$fileRootPath/$file"

  val sparkConf =  {
    val baseConf =
      new SparkConf()
        .setAppName("SampleDbIngestionWithCDC")

    if( (args.length >= 1) && (args(0) == "local") ) {
      baseConf
        .setMaster("local")
    } else {
      baseConf
    }
  }
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val dbConnectionUrl = s"jdbc:oracle:thin:$dbServerConnection/$dbName"

  println(s"Connecting to '$dbConnectionUrl'")
  println(s"Reading from '$fileRootPath'")

  val customerTableDataFrame =
    sqlContext
      .load(
        "jdbc",
        Map(
          "url" -> dbConnectionUrl,
          "dbtable" -> "customer"
        )
      )

  import uk.co.pragmasoft.experiments.bigdata.spark.cdc.CustomerOperations._

  val newCustomerData = customerTableDataFrame
    .map { row => CustomerData(row.getString(0),row.getString(1), Option(row.getString(2))) }

  val prevSnapshotBeforeValidation =
    sc.textFile( filePath("fullData.csv") )
      .extractCSV( skipHeader = true )
      .parseAsCustomers
      .cache()


  val previousSnapshot =
    prevSnapshotBeforeValidation
      .extractValid

  prevSnapshotBeforeValidation
    .extractInvalid
    .map( lineWithErrorDescription => s"""${lineWithErrorDescription._1}: errors: '${lineWithErrorDescription._2.mkString(",")}'""" )
    .saveAsTextFile(filePath("out/errors.txt"))

  computeCdc( { customer: CustomerData => customer.customerId } )(newCustomerData, previousSnapshot)
    .map( customerCdc => printAsStringArray(customerCdc)(CustomerData.asStringArray) )
    .addHeader(sc)( "customerId", "name", "address", "cdc" )
    .writeAsCsv( filePath("out/cdc.csv") )


}
