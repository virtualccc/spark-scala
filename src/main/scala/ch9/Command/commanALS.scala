package ch9.Command



import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object commanALS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LodeCodeSet").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val K = 10 //10个菜品

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
    //对推荐结果中的用户与菜品编码进行反规约操作(usercode ,userID)最终
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


    //基于als模型推荐，向用户编码=1000，推荐10份新菜
    //加载模型
    val modelPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\ALSComand\\model"
    val model = MatrixFactorizationModel.load(sc, modelPath)
    model.productFeatures.cache
    model.userFeatures.cache
    println("model retrieved")


    //加载训练数据集
    val trainDataPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\trainRatings"
    val trainData = sc.textFile(trainDataPath).map { x =>
      val fields = x.slice(1, x.size - 1).split(",");
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }

    //用户编码1000评价过的菜
    val user1000RateMealIds = trainData.filter(_._1 == 1000).map(_._2).collect.toSet

    //用户编码1000未评价过的菜
    val cands1 = trainData.map(x => x._2).filter(!user1000RateMealIds.contains(_)).distinct()


    //推荐10份菜
    val recommendation1 = model.predict(cands1.map((1000, _))).collect.sortBy(-_.rating).take(K)


    val recommendation2 = recommendation1.map(x => x.toString())
    val recommendation = recommendation2.map {
      x =>
        val tmp1x = x.substring(x.indexOf("(") + 1, x.indexOf(")") - 1);
        val tmp2x = tmp1x.split(",");
        (tmp2x(0).toInt, tmp2x(1).toInt, tmp2x(2).toDouble)
    }
    val recommendationList = recommendation.map {
      case (user: Int, meal: Int, rating: Double) => (user, meal, rating)
    }
    //反编码匹配
    val recommendationRecords = recommendationList.map {
      case (user, meal, rating) => (reverseUserZipCode.get(user).get, reverseMealZipCode.get(meal).get, rating)
    }


    //引用真实菜名，最终推荐结果
    val realRecommendationRecords = recommendationRecords.map {
      case (user, meal, rating) => (user, meal, mealsMap.get(meal).get, rating)
    }.foreach(println)
    println("-------------------------------------")
    val test1 = model.recommendProducts(1000, K); //调用topN推荐

      {//反编码对应输出代码块
    val test = test1.map(x => x.toString()).map {
      x =>
        val tmp1x = x.substring(x.indexOf("(") + 1, x.indexOf(")") - 1);
        val tmp2x = tmp1x.split(",");
        (tmp2x(0).toInt, tmp2x(1).toInt, tmp2x(2).toDouble)
    }
    val testList = test.map {
      case (user: Int, meal: Int, rating: Double) => (user, meal, rating)
    }
    //反编码匹配
    val testRecord = testList.map {
      case (user, meal, rating) => (reverseUserZipCode.get(user).get, reverseMealZipCode.get(meal).get, rating)
    }
    //引用真实菜名，最终推荐结果
    val testRecords = testRecord.map {
      case (user, meal, rating) => (user, meal, mealsMap.get(meal).get, rating)
    }.foreach(println)
     }

    println(model.predict(1000,1))//用户，物品评分

//    model.recommendProductsForUsers(K)//对所有用户推荐K个物品；
//    model.recommendUsersForProducts(K)//对所有物品推荐K个用户

  }

}

