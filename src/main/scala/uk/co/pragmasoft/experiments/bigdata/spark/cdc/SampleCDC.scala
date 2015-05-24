package uk.co.pragmasoft.experiments.bigdata.spark.cdc

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import uk.co.pragmasoft.experiments.bigdata.spark.{CustomerData, CSVOperations}


object SampleCDC extends App with CdcSupport {

  def filePath(file: String) = s"$fileRoot/$file" //new File(fileRoot, file).getAbsolutePath

  import CSVOperations._
  import Cdc._
  import CustomerOperations._

  val sparkConf = new SparkConf().setAppName("SampleCDC")
  val sc = new SparkContext(sparkConf)

  val fileRootPath = if(args.length > 0) args(0) else "data/cdc"

  lazy val fileRoot = new File(fileRootPath)

  val inputFile = filePath("newData.csv")

  println(s"Reading from $inputFile")

  val newData = sc.textFile( inputFile ).extractCSV( skipHeader = true ).persist()

  val newCustomerData =
    newData
      .extractValidCustomers


  val previousSnapshot =
    sc.textFile( filePath("fullData.csv") )
      .extractCSV( skipHeader = true )
      .extractValidCustomers


  computeCdc( { customer: CustomerData => customer.customerId } )(newCustomerData, previousSnapshot)
    .map( customerCdc => printAsStringArray(customerCdc)(CustomerData.asStringArray) )
    .addHeader(sc)( "customerId", "name", "address", "cdc" )
    .writeAsCsv( filePath("out/cdc.csv") )


  val faultyRecords = newData.extractInvalidRecords

  faultyRecords
    .map( line => Array(line._1, line._2.mkString("\"", ",", "\"") ) )
    .addHeader(sc)("Error description", "Line")
    .writeAsCsv( filePath("out/faultyRecords.csv") )

}
