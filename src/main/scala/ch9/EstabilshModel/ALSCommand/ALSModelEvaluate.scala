package ch9.EstabilshModel.ALSCommand

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对ALS推荐模型进行评测
  * 输入参数： <trainDataPath> <testDataPath><modelPath><minRatedNumPerUser><kList><resultPath><splitter>
  * trainDataPath： 过滤后的训练数据存储路径
  * testDataPath： 测试数据存储路径
  * modelPath： ALS模型路径
  * minRatedNumPerUser:单用户评价菜品的最小次数
  * kList: 推荐数量K值列表
  * resultPath：结果输出路径
  * splitter：原始数据分隔符；
  */
object ALSModelEvaluate {


   def main(args:Array[String]) = {
//     if (args.length != 7) {
//       System.err.println("Usage:ALSModelEvaluate requires: <trainDataPath> <testDataPath><modelPath><minRatedNumPerUser><kList><resultPath><splitter>")
//     }

     val appName = "Evaluate Spark ALS Model"
     val conf = new SparkConf().setAppName(appName).setMaster("local[2]")
     val sc = new SparkContext(conf)
     sc.setCheckpointDir("checkpoint")
     sc.setLogLevel("WARN")

     // 匹配输入参数
     val trainDataPath ="E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\trainRatings"
     val testDataPath ="E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\testRatings"
     val modelPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\ALSComand\\model"
     sc.setCheckpointDir("checkpoint")
     val minRatedNumPerUser = 1
     val arg4="10,20,30,40,50"
     val kList = arg4.split(",").map(_.toInt)
     val resultPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\ALSComand\\res"
     val splitter = ","

     // 加载 ALS model
     val model = MatrixFactorizationModel.load(sc, modelPath)
     model.productFeatures.cache
     model.userFeatures.cache
     println("model retrieved.")

     // 加载训练集数据
     val trainData = sc.textFile(trainDataPath).map{x=>val fields=x.slice(1,x.size-1).split(splitter);
       (fields(0).toInt,fields(1).toInt,fields(2).toDouble)}

     // 加载测试集数据
     val testData = sc.textFile(testDataPath).map{x=>val fields=x.slice(1,x.size-1).split(splitter); (fields(0).toInt,fields(1).toInt)}
     val testDataFiltered = testData.groupBy(_._1).filter(data=>data._2.toList.size>=minRatedNumPerUser).flatMap(_._2).distinct()

     println("filtered test records  count : " + testDataFiltered.count) //
     val testUserList = testData.keys.distinct().collect().toList
     val testRecordSet = testData.distinct().collect.toSet
     println("All test users: " + testUserList.size)
     println("All test records: " + testRecordSet.size)

     // 计算训练集与测试集中的共有用户，以及测试集中的相关记录。
     val sharedUserList = trainData.map(_._1).distinct.collect.toList.intersect(testDataFiltered.map(_._1).distinct.collect.toList)
     val testSharedSet = testDataFiltered.filter(data=>sharedUserList.contains(data._1)).collect.toSet
     println("ShareUsers: " + sharedUserList.size)
     println("Test Records: " + testSharedSet.size)

     // 创建评测结果集
     val evaluationRecords = new scala.collection.mutable.ListBuffer[(Int,Double,Double,Double)]()

     // 评测model推荐的结果
     // 计算不同Ｋ值下的recall,precision
     for (k <- kList){
       println("================== K="+k+" ==================")
       var recommendNumSum=0
       var matchedNumSum = 0
       for(uid <- sharedUserList){
         //recommendProducts测试函数
         val initRecommendRecords = model.recommendProducts(uid, k).map { case Rating(user, item, rating) => (user, item) }
         // 在剩余的推荐数据集中，匹配测试集中的总数
         val matchedNum = initRecommendRecords.toSet.intersect(testSharedSet).size
         matchedNumSum += matchedNum
       }
       recommendNumSum = sharedUserList.size*k
       val recall: Double = (matchedNumSum.toDouble / testSharedSet.size) * 100
       val precision: Double = (matchedNumSum.toDouble / recommendNumSum) * 100
       val F1:Double=(recall * precision * 2) /(recall + precision)
       evaluationRecords +=((k,recall,precision,F1))
       println(k + ": "+ sharedUserList.size + ": " + matchedNumSum + ": " + recall + ": "+ precision + ":"+ F1)
     }
     val result = sc.parallelize(evaluationRecords.toSeq).repartition(1)
     result.saveAsTextFile(resultPath)
     sc.stop()
   }
 }

/**
  * filtered test records  count : 7426
  * All test users: 3296
  * All test records: 7426
  * ShareUsers: 3215
  * Test Records: 6944
  *
================== K=10 ==================
10: 3215: 70: 1.0080645161290323: 0.2177293934681182:0.3581112191129074
================== K=20 ==================
20: 3215: 139: 2.0017281105990783: 0.2161741835147745:0.390208298242659
================== K=30 ==================
30: 3215: 203: 2.9233870967741935: 0.21047174701918095:0.3926726889374626
================== K=40 ==================
40: 3215: 277: 3.9890552995391704: 0.21539657853810265:0.40872336658206937
================== K=50 ==================
50: 3215: 347: 4.997119815668203: 0.21586314152410574:0.41384903455102745
  */
/**
  * 从综合指标F1来看，当最小评价数为2，K值为10时，各个模型推荐表现最好
  *       若菜品数远小于用户数，表明用户数据分布与菜品数据分布比较时，前者更加稀疏
  *
  * 用户协同过滤，适合社交关系网络产品推荐，如QQ，微信
  *
  * 基于物品的协同过滤，更适用与非社交关系网络下，更适合用历史物品数据来做
  *
  * 本案例使用基于物品协同过滤比较合适
  *
  * 计算复杂度
  *     用户推荐，必须以用户数为基数计算相似度矩阵，用户越多矩阵越大，计算复杂（新闻，电商产品推荐）
  *
  *     物品推荐同理（用户大，物品少）电影、书籍等
  *
  *     ALS推荐模型，在建模时进行降维，更适合于大规模数据计算
  *
  *选择以基于物品为主，并基于ALS推荐模型为辅
  *
  *
  */

