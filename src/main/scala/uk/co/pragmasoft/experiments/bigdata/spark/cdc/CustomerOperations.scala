package uk.co.pragmasoft.experiments.bigdata.spark.cdc

import org.apache.spark.rdd.RDD
import uk.co.pragmasoft.experiments.bigdata.spark.CustomerData

object CustomerOperations {
  import CustomerData._

  implicit class WithParsedTextRecordOperations(val rdd: RDD[Array[String]]) extends AnyVal {
    def parseAsCustomers : RDD[ Either[LineWithErrorDescription, CustomerData] ] =  rdd.map( CustomerData.fromStringArray )

    def extractValidCustomers : RDD[CustomerData] = parseAsCustomers.filter( _.isRight ).map( _.right.get )

    def extractInvalidRecords : RDD[LineWithErrorDescription] = parseAsCustomers.filter( _.isLeft ).map( _.left.get )
  }
}
