package uk.co.pragmasoft.experiments.bigdata.spark.cdc

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER

import scala.reflect.ClassTag

trait CdcSupport {

  import Cdc._

  def computeCdc[Key, Value](keyExtractor: Value => Key)(newSnapshot: RDD[Value], previousSnapshot: RDD[Value])(implicit kt: ClassTag[Key], vt: ClassTag[Value]): RDD[Cdc[Value]] = {

    println( s"New snapshot $newSnapshot")
    println( s"Previous snapshot $previousSnapshot")

    val newByKey =
      newSnapshot
        .filter( _ != null)
        .keyBy( keyExtractor )
        .persist(MEMORY_AND_DISK_SER)

    val previousByKey =
      previousSnapshot
        .filter( _ != null)
        .keyBy( keyExtractor )
        .persist(MEMORY_AND_DISK_SER)

    val deletedRecords = extractDeletedRecords(newByKey, previousByKey)

    val otherOps =
      extractNewAndUpdated(newByKey, previousByKey)

    otherOps union deletedRecords
  }

  protected def extractNewAndUpdated[Key, Value](newSnapshot: RDD[(Key, Value)], previousSnapshot: RDD[(Key, Value)])(implicit kt: ClassTag[Key], vt: ClassTag[Value]): RDD[Cdc[Value]] = {
    newSnapshot
      .leftOuterJoin(previousSnapshot)
      .map {
        case (recordId, (currentInfo, Some(previousInfo))) =>
          if (currentInfo == previousInfo)
            noop(currentInfo)
          else
            updated(currentInfo)

        case (_, (currentInfo, None)) =>
            inserted(currentInfo)
      }
      .filter(cdc => !isNoop(cdc))
  }

  protected def extractDeletedRecords[Key, Value](newSnapshot: RDD[(Key, Value)], previousSnapshot: RDD[(Key, Value)])(implicit kt: ClassTag[Key], vt: ClassTag[Value]): RDD[Cdc[Value]] = {
    newSnapshot
      .rightOuterJoin(previousSnapshot)
      .filter {
        case (_, (None, _)) => true
        case _ => false
      }
      .map { case (_, (_, value)) =>
        deleted(value)
      }
  }
}
