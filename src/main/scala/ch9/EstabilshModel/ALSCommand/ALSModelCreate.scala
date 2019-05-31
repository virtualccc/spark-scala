package ch9.EstabilshModel.ALSCommand

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating


object ALSModelCreate {
  val appName = "Create ALS Model "
  val conf = new SparkConf().setAppName(appName).setMaster("local[3]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  def main(args: Array[String]) = {
//    if (args.length != 6) {
//      System.err.println("Usage: ALSModelCreate requires: 6 input fields <trainDataPath> <modelPath>  " +
//        "<rank> <iteration> <lambda> <splitter>")
//    }
    // 匹配输入参数
    val trainDataPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\trainRatings"
    val modelPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\ALSComand\\model"
    sc.setCheckpointDir("checkpoint")
    //以下数据通过ALSModelOptimize训练得出
    val rank = 20
    val iteration = 20
    val lambda = 0.3
    val splitter = ","
    /**
      * 创建ALS模型，需要输入以下参数
      * trainDataPath: 输入原始数据，包含（用户，项目，评分）
      * modelPath：模型存储目录
      * rank ： 用户、项目子矩阵
      * iteration： 循环次数；
      * lambda ： 防止过拟合参数；
      * splitter： 输入原始数据分隔符；
      */

    // 加载训练集数据
    val trainData = sc.textFile(trainDataPath).map{x=>val fields=x.slice(1,x.size-1).split(splitter);
      (fields(0).toInt,fields(1).toInt,fields(2).toDouble)}
    val trainDataRating= trainData.map(x=>Rating(x._1,x._2,x._3))

    // 建立ALS模型
    val model = ALS.train(trainDataRating, rank, iteration, lambda)
    // 存储ALS模型
    model.save(sc,modelPath)
    println("Model saved")
    sc.stop()
  }
}