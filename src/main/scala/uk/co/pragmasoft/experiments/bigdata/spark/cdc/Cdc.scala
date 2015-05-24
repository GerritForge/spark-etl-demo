package uk.co.pragmasoft.experiments.bigdata.spark.cdc

/**
 * Created by stefano on 26/04/15.
 */
object Cdc {
   def inserted[T](record: T) = new Cdc(record, "I")
   def deleted[T](record: T) = new Cdc(record, "D")
   def updated[T](record: T) = new Cdc(record, "U")
   def noop[T](record: T) = new Cdc(record, "")

   def isNoop(cdc: Cdc[_]): Boolean = cdc.operation == ""


   def printAsStringArray[T](cdc: Cdc[T])(implicit recordExtractor: T => Array[String]): Array[String] = recordExtractor(cdc.record) ++ Array(cdc.operation)
 }

class Cdc[T] private (val record: T, val operation: String) extends Serializable