package ch4

import org.apache.spark.{SparkConf, SparkContext}

/**
  * partitionBy方式分区,自定义
  * coalesce(分区数，shuffle:Boolean=false[不指定为false无法分区比原来多])
  * repartition（就是上面不指定shuffle）
  */


object ch4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val input = sc.textFile("K:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第4章\\任务程序\\data/words.txt")

    val count = input.flatMap(x=>x.split(" ")).map((_,1)).reduceByKey(_+_)

    val data1= count.partitionBy(new MyPartition(2))

    count.foreach(x=>println(x._1+","+x._2))

  }

}
