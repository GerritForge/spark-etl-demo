package uk.co.pragmasoft.experiments.bigdata.spark

import org.scalatest.{FlatSpec, Matchers}

class CSVOperationsSpec extends FlatSpec with Matchers with SparkTestSupport {

  import CSVOperations._

  behavior of "CSVReaderOperations"

  it should "Convert an RDD of strings into an RDD of Array[String] splitting the values with comma" in withSparkContext { sc =>
    val stringRdd =  sc.parallelize(  List( "1field1,1field2,1field3", "2field1,2field2,2field3" ) )

    stringRdd.extractCSV().collect() shouldEqual ( Array( Array("1field1" ,"1field2", "1field3" ), Array("2field1" ,"2field2", "2field3" ) ) )
  }

  it should "Strip header if asked so" in withSparkContext { sc =>
    val stringRdd =  sc.parallelize(  List( "header1,header2,header3", "1field1,1field2,1field3", "2field1,2field2,2field3" ) )

    stringRdd.extractCSV(true).collect() shouldEqual ( Array( Array("1field1" ,"1field2", "1field3" ), Array("2field1" ,"2field2", "2field3" ) ) )
  }

  behavior of "CSVWriterOperations"

}
