package uk.co.pragmasoft.experiments.bigdata.spark.cdc

/**
 * Created by stefano on 26/04/15.
 */
trait Cdc[+T] {
  def operation: String
  def record: T
}

object Cdc {
  private case class CdcImpl[+T](val record: T, val operation: String) extends Cdc[T]

  def inserted[T](record: T): Cdc[T] = new CdcImpl(record, "I")
  def deleted[T](record: T): Cdc[T] = new CdcImpl(record, "D")
  def updated[T](record: T): Cdc[T] = new CdcImpl(record, "U")
  def noop[T](record: T): Cdc[T] = new CdcImpl(record, "")

  def isNoop(cdc: Cdc[_]): Boolean = cdc.operation == ""

  def printAsStringArray[T](cdc: Cdc[T])(implicit recordExtractor: T => Array[String]): Array[String] = recordExtractor(cdc.record) ++ Array(cdc.operation)
}



