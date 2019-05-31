package ch9

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object CodeDate {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("DataLoadHive").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hiveCtx= new HiveContext(sc)

    val inputPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\data1"

    val cleanMealData = hiveCtx.jsonFile(inputPath)

    //选取属性，生成用户评分数据集

    val MealRatings = cleanMealData select( "UserID", "MealID", "Rating", "ReviewTime")
    //QLContext读取一个文件之后,返回DataFrame类型的变量可以直接.map操作版本升级需要加rdd

    val RatingRDD = MealRatings.rdd.map(row => (row(0).toString, row(1).toString,row(2).toString, row(3).toString))
    //构造用户、菜品的编码集
    val userZipCode = RatingRDD.map(_._1).distinct().sortBy(x=>x).
        //zipWithIndex加一个自增量变形(x,0),(y,1)
      zipWithIndex().map(x=>(x._1,x._2.toInt))
    val mealZipCode = RatingRDD.map(_._2).distinct().sortBy(x=>x).
      zipWithIndex().map(x=>(x._1,x._2.toInt))
    //tomap后  x -> 0,y -> 1
    val userZipCodeMap = userZipCode.collect().toMap
    val mealZipCodeMap = mealZipCode.collect().toMap

    //以用户菜品编码，重新构造评分数据集       (2580,361,5.0,1496177056)//将用户名与菜名进行模式匹配，x.3为评分，x.4为时间戳
    val RatingCodeList = RatingRDD.map(x=>(userZipCodeMap(x._1),mealZipCodeMap(x._2),x._3,x._4))
    RatingCodeList.take(5)
    //储存
    userZipCode.repartition(1).saveAsTextFile("E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\userZipCode")
    mealZipCode.repartition(1).saveAsTextFile("E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\mealZipCode")
    RatingCodeList.repartition(1).saveAsTextFile("E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\RatingCodeList")


  }

}
