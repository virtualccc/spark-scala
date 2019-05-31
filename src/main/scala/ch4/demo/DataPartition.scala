package ch4.demo

import org.apache.spark.Partitioner

class DataPartition extends Partitioner{
  //由于只有4个年份，分区设置为4个
  override def numPartitions: Int = 4

  override def getPartition(key: Any): Int = {
    //前4个代表年份
    val date = key.toString.substring(0,4).trim()
    if (date=="2013") {
      0
    }
    else if(date=="2014"){
      1
    }
    else if(date=="2015"){
      2
    }else{
      3
    }
  }

  override def equals(other: Any): Boolean = other match {
      // case 第一个为任意名字：类名=>名字.numPartitions == numPartitions
//      后面不变
    case dataPartition: DataPartition =>
      dataPartition.numPartitions == numPartitions
    case _ =>
      false
  }
}
