package ch9

import org.apache.spark.{SparkConf, SparkContext}


object Dataspilt {
  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("DataLoadHive").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val inputPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\RatingCodeList"

    val RatingCodeList=sc.textFile(inputPath,6).map {x=>
      //去掉了括号
      val fields = x.slice(1,x.size-1).split(",");
      (fields(0).toInt,fields(1).toInt,fields(2).toDouble,fields(3).toLong)
    }.sortBy(_._4)//根据时间戳排序

//zipWithIndex在RDD中的ID（索引号）组合成键/值对。(A,0), (B,1), (R,2), (D,3), (F,4)
    //(2271,1430,4.0,1498724800) ->1
//    对键值对每个value都应用一个函数，但是，key不会发生变化。
    val zipRatingCodeList = RatingCodeList.zipWithIndex().mapValues(x=>(x+1))
    //定义数据集分割点
    val totalNum = RatingCodeList.count()
    val splitPoint1 = totalNum * 0.8 toInt
    val splitPoint2 = totalNum * 0.9 toInt
    //生成训练数据
    //不要时间戳
    val train = zipRatingCodeList.filter(x=>(x._2<splitPoint1)).map(x=> (x._1._1,x._1._2,x._1._3))
    //生成验证数据集
    val validate = zipRatingCodeList.filter(x=>(x._2>=splitPoint1 && x._2<splitPoint2)).map(x=> (x._1._1,x._1._2,x._1._3))
    //生成测试数据集
    val test = zipRatingCodeList.filter(x=>(x._2>=splitPoint1)).map(x=> (x._1._1,x._1._2,x._1._3))
    //存储
    val outputpath="E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序"
    train.repartition(6).saveAsTextFile(outputpath+"/trainRatings")
    validate.repartition(6).saveAsTextFile(outputpath+"/validateRatings")
    test.repartition(6).saveAsTextFile(outputpath+"/testRatings")
  }
}
