package uk.co.pragmasoft.experiments.bigdata.spark // Luca's fix

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CSVOperations {
  private def stringToCsvRecord(row: String): Array[String] = row.split(',')

  implicit class CSVReaderOperations(val rdd:RDD [String]) extends AnyVal {
    def extractCSV(skipHeader: Boolean = false): RDD[Array[String]] = {
      val withOptionallyStrippedHeader =
        if (skipHeader) {
          rdd.zipWithIndex().filter {
            case (_, index) => index > 0
          } map ( _._1 )
        } else {
          rdd
        }

      withOptionallyStrippedHeader map stringToCsvRecord
    }
  }

  implicit class CSVWriterOperations(val rdd: RDD[Array[String]]) extends AnyVal {
    def addHeader(sparkContext: SparkContext)(header: String*): RDD[Array[String]] = {
      sparkContext.parallelize( Seq(header.toArray) ).union(rdd)
    }

    def writeAsCsv(destinationPath: String) {
      rdd
        .map( _.mkString(",") )
        .saveAsTextFile(destinationPath)
    }
  }
}

