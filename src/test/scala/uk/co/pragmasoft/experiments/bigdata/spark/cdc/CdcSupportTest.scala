package uk.co.pragmasoft.experiments.bigdata.spark.cdc

import org.scalatest.{Matchers, FlatSpec}
import uk.co.pragmasoft.experiments.bigdata.spark.SparkTestSupport
import Cdc._

case class Record(key: Int, value: String)

class CdcSupportTest extends FlatSpec with Matchers with SparkTestSupport with CdcSupport {

  behavior of "extractDeletedRecords"

  it should "extract as deleted record the ones with keys present in the previousSnapshot but not in the new one" in withSparkContext { sc =>

    val previousSnapshot = sc.parallelize( List( Record(1, "value"), Record(2, "value 2"), Record(3, "value") ) ).keyBy( _.key )
    val newSnapshot = sc.parallelize( List( Record(1, "new value"), Record(3, "value") ) ).keyBy( _.key )

    val deletedRecords = extractDeletedRecords(newSnapshot, previousSnapshot)

    deletedRecords.collect().toList should be ( List(deleted(Record(2, "value 2"))) )
  }

  behavior of "extractNewAndUpdated"

  it should "Extract as NEW records the ones with keys not present in previous but present in new snapshot" in withSparkContext { sc =>
    val previousSnapshot  = sc.parallelize( List( Record(1, "value"), Record(3, "value") ) ).keyBy( _.key )
    val newSnapshot= sc.parallelize( List( Record(1, "value"), Record(2, "value 2"), Record(3, "value") ) ).keyBy( _.key )

    val deletedRecords = extractNewAndUpdated(newSnapshot, previousSnapshot)

    deletedRecords.collect().toList should be ( List(inserted(Record(2, "value 2"))) )

  }

  it should "Extract as UPDATED records the ones with keys present both snapshots but with different values and return new value" in withSparkContext { sc =>
    val previousSnapshot  = sc.parallelize( List( Record(1, "value"), Record(3, "value") ) ).keyBy( _.key )
    val newSnapshot= sc.parallelize( List( Record(1, "value"), Record(3, "new value") ) ).keyBy( _.key )

    val deletedRecords = extractNewAndUpdated(newSnapshot, previousSnapshot)

    deletedRecords.collect().toList should be ( List(updated(Record(3, "new value"))) )
  }

  behavior of "computeCdc"

  it should "Detect updated, inserted and deleted records composing the other two functions, unaltered records should be ignored" in withSparkContext { sc =>
    val record1 = Record(1, "value")
    val record2 = Record(2, "value")
    val record3 = Record(3, "value")
    val record3Updated: Record = record3.copy(value = "new value")
    val record4: Record = Record(4, "value")

    val previousSnapshot  = sc.parallelize( List( record1, record2, record3 ) )
    val newSnapshot= sc.parallelize( List( record1, record3Updated, record4 ) )

    val cdc = computeCdc( (record: Record) => record.key )(newSnapshot, previousSnapshot)

    cdc.collect().toSet should be ( Set( deleted(record2), updated(record3Updated), inserted(record4)  ) )
  }
}
