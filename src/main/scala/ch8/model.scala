package ch8

import org.apache.spark.{SparkConf, SparkContext}

object model {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TFIDF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input1 = "D:\\新建文件夹\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第8章\\任务程序\\data/"

    val data= sc.textFile(input1+"tf-idf.txt").map(_.split(" ").toSeq)

  }



}
