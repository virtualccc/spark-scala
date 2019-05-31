package ch9.Command

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scala.math._

object commanBaseItem {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LodeCodeSet").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val   K=10;//10个菜品
    val userZipCodePath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\userZipCode"
    val mealZipCodePath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\mealZipCode"

    val userZipCode = sc.textFile(userZipCodePath).map { x =>
      val fields = x.slice(1, x.size - 1).split(",");
      (fields(0), fields(1).toInt)
    }

    val mealZipCode = sc.textFile(mealZipCodePath).map { x =>
      val fields = x.slice(1, x.size - 1).split(",");
      (fields(0), fields(1).toInt)
    }
    //对推荐结果中的用户与菜品编码进行反规约操作(userid,mealid)最终
    val reverseUserZipCode = userZipCode.map(x => (x._2, x._1)).collect().toMap
    val reverseMealZipCode = mealZipCode.map(x => (x._2, x._1)).collect().toMap

    //加载外部数据库中，菜品数据，通过DF查询方法生成map格式的数据对
    val sqlContext = new SQLContext(sc)
    val url = "jdbc:mysql://192.168.27.1:3306/MealData?characterEncoding=UTF-8"
    val mealDF = sqlContext.read.format("jdbc").options(
      Map("url" -> url, "user" -> "root", "password" -> "root", "dbtable" -> "meal_list")
    ).load()

    //生成菜品数据
    val mealsMap = mealDF.select("mealID", "meal_name").rdd.map(x => (x.getString(0), x.getString(1))).collect().toMap


    //加载训练数据
    val trainDataPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\trainRatings"

    val trainData = sc.textFile(trainDataPath).map{
      x=>val fields =x.slice(1,x.size-1).split(",");
        (fields(0).toInt, fields(1).toInt)
    }

    val trainUserRated = trainData.combineByKey(
      (x:Int) => List(x),
      (c:List[Int], x:Int) => c :+ x ,
      (c1:List[Int], c2:List[Int]) => c1 ::: c2).cache()

    //加载模型
    val modelPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\ITEMBASE/model"
    val dataModel:RDD[(Int,List[Int])]=sc.objectFile[(Int,List[Int])](modelPath)

    //过滤训练数据中已有的菜品，生成可推荐的新菜品
    val dataModelNew = dataModel.join(trainUserRated).map(x=> (x._1,(x._2._1.diff(x._2._2))))
    //为用户1000推荐10份菜
    val recommendation = dataModelNew.map(
      x=> (x._1,x._2.take(K)))
      .filter(x=>(x._1==1000)).flatMap(x=>x._2.map(y=>(x._1,y)))//去掉filter就是对所有用户推荐
    //反编码推荐后的结果

    //反编码匹配
    val recommendationRecords = recommendation.map {
      case (user, meal) => (reverseUserZipCode.get(user).get, reverseMealZipCode.get(meal).get)
    }

    //引用真实菜名，最终推荐结果
    val realRecommendationRecords = recommendationRecords.map {
      case (user, meal) => (user, meal, mealsMap.get(meal).get)
    }.foreach(println)

  }
}
