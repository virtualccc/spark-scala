package ch4.demo

import java.text.SimpleDateFormat

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 通过简单平移预测下一期预测值,还有指数移动，加权等
  *
  * F = （A1+A2+...+AN）/N
  */
object MovingAverage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MovingAverage").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input = "K:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第4章\\任务程序\\data\\reform_data/000002.csv"
    val output = "K:\\res"
    //定义文本分隔符
    val splitter = ","

    //时间窗
    val brodcastWindow:Int = 10

    val output2 = "K:\\res1"

    val fm = new SimpleDateFormat("yyyy-MM-dd")
    //采用开盘价做预测
    val data = sc.textFile(input).filter(x => !x.contains("date")).map{x=>
      val split_data = x.split(splitter)
      val timestamp = fm.parse(split_data(0))
      (timestamp.getTime(),(split_data(0),split_data(1).toDouble))
    }.sortByKey().map(x=>(x._2._1,x._2._2))
    val queue = new mutable.Queue[Double]()

    val yucezhangfu = data.map{x=>
      queue.enqueue(x._2)
      //当窗口超过10个数，对队列求和，减去最后加入的数，再删除第一个
      if(queue.size>brodcastWindow){
        val ave = (queue.sum-x._2)/brodcastWindow;
        queue.dequeue();
        (x._1,ave,x._2)
      }
      else{(0,0,0)}
      //持久化方式
    }.filter(x=>x._1!=0).persist(StorageLevel.MEMORY_AND_DISK)

    //结果根据日期分区
//    yucezhangfu.map(x=>(x._1,(x._2,x._3))).partitionBy(new DataPartition()).saveAsTextFile(output)
//    yucezhangfu.map(x=>(x._1,x._2)).partitionBy(new DataPartition()).saveAsTextFile(output2)
    yucezhangfu.map(x=>(x._1,(x._2,x._3))).partitionBy(new DataPartition()).foreach(x=>println(x))
    yucezhangfu.map(x=>(x._1,x._2)).partitionBy(new DataPartition()).foreach(x=>println(x))




  }
}
