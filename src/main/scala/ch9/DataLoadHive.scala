package ch9

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/*
映射在外部的hive中
 */
object DataLoadHive {
  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setAppName("DataLoadHive").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val hiveCtx= new HiveContext(sc)

    val inputPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\data/MealRatings_201705_201706.json"

    //3万8千条数据，,USERID,rating,revietime,review,mealID,json

    val mealData =hiveCtx.jsonFile(inputPath)
    //临时注册一张表
    mealData.registerTempTable("mealData")

    val mealresults = hiveCtx.sql("select UserID,MealID,Rating,Review,ReviewTime from mealData")

    mealresults.show(5)

//    //    总数记录
//    hiveCtx.sql("select count(*) as Records_Sum from mealData ").show()
//    //总用户
//    hiveCtx.sql("select count(distinct UserID) as User_Num from mealData ").show()
//    //总菜品
//    hiveCtx.sql("select count(distinct MealID) as Meal_Sum from mealData ").show()
//    //最低评分与最高
//    hiveCtx.sql("select MIN(Rating) as MIN_Rating,MAX(Rating) as MAX_Rating from mealData ").show()
//    //各级评分分组统计
//    hiveCtx.sql("select Rating,count(*) as Num from mealData group by Rating order by Rating")
//    //用户的评分次数统计,返回评分次数最多的前5名用户
//    hiveCtx.sql("select UserID,count(*) as Num from mealData group by UserID order by Num desc").show(5)
//    //用户的评分次数统计,返回评分次数最多的前5名菜品
//    hiveCtx.sql("select MealID,count(*) as Num from mealData group by MealID order by Num desc").show(5)

////    统计最早评分和最晚评分日期,From_Unixtime时间戳转换时间
//    hiveCtx.sql("select MIN(From_Unixtime(ReviewTime)) as StartDate, MAX(From_Unixtime(ReviewTime)) as ENDDate from mealData").show()
//
//    //按日期统计用户评分分布
//    val mealDataWithData= hiveCtx.sql("select *,(From_Unixtime(ReviewTime,'yyyy-mm-dd')) as ReviewDate from mealData")
//    mealDataWithData.registerTempTable("mealDataWithData")
//    hiveCtx.sql("select ReviewDate,count(*) as Num from mealDataWithData group by ReviewDate order by ReviewDate").show(10)

//
////    查询重复评分记录总数
////    将重复评分记录保存在临时表
//    val repeatedRatings=hiveCtx.sql("select UserID,MealID,count(*) as Num from mealData group by UserID,MealID Having Num>1 order by Num desc")
//    repeatedRatings.registerTempTable("repeatedRatings")
//
//    val repeatedRatingsList= hiveCtx.sql("select a.* from mealData a join repeatedRatings b where a.UserID = b.UserID and a.MealID = b.MealID order by a.MealID,a.UserID")
//    repeatedRatingsList.select("UserID","MealID","Rating","ReviewTime").show(10)


    //删除重复评分数据
    val latestRatingPair = hiveCtx.sql("select UserID,MealID,MAX(ReviewTime) as LatestDate from mealData group by UserID,MealID")
    latestRatingPair.registerTempTable("latestRatingPair")
    val latestRatings = hiveCtx.sql("select a.UserID,a.MealID,a.Rating,a.ReviewTime from mealData a join latestRatingPair b "+
      "where a.UserID=b.UserID and  a.MealID =b.MealID  and a.ReviewTime =b.LatestDate ")

    //保存去重后评分
    val outputpath="E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\data1"

    //保存为json格式
    latestRatings.write.mode(SaveMode.Overwrite).json(outputpath)




  }


}
