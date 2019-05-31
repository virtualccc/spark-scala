package ch4

import org.apache.spark.Partitioner

class MyPartition(numParts:Int) extends Partitioner{
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    if (key.toString().toInt%2==0) {
      0
    } else {
      1
    }
  }
  override def equals(other: Any): Boolean = other match {
    case mypartition: MyPartition =>
      mypartition.numPartitions == numPartitions
    case _ =>
      false
  }
}