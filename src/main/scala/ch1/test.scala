package ch1

import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    val conf =  new SparkConf().setMaster("local[2]").setAppName("test")
    val sc = new SparkContext(conf)
    var a = sc.parallelize(List(1,2))
    var b = sc.parallelize(List("A","B","C"))
    a.cartesian(b).foreach(print)
  }

}
