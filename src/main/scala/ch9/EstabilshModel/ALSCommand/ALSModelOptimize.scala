package ch9.EstabilshModel.ALSCommand
/**

  *
  * 3.在开发工具(intell)的vm窗口加入参数:
  -Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.NativeRefBLAS
  -Dcom.github.fommil.netlib.LAPACK=com.github.fommil.netlib.NativeRefLAPACK
  -Dcom.github.fommil.netlib.ARPACK=com.github.fommil.netlib.NativeRefARPACK
  -Dcom.github.fommil.netlib.NativeRefBLAS=E:\spark\netlib-native_ref-win-x86_64.so
  -Dcom.github.fommil.netlib.NativeSystemBLAS.natives=E:\spark\netlib-native_system-win-x86_64.so
  */
/**
  * stackoverflow
  *
  * jvm设置的问题-Xss1M。。。很显然也没有进展。
  *
  * spark在迭代计算的过程中，会导致linage剧烈变长，所需的栈空间也急剧上升，最终爆栈了。。
  *
  * 这类问题解决方法如下：
  *
  * 在代码中加入 sc.setCheckpointDir（path），显示指明checkpoint路径，问题便可得到解决。
  */

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


object ALSModelOptimize {

  // 定义计算均方根误差的函数：computeRMSE
  def computeRMSE(model:MatrixFactorizationModel, data:RDD[Rating]): Double = {
    val usersProducts = data.map(x=>(x.user,x.product))
    val ratingsAndPredictions = data.map{case Rating(user,product,rating)=>((user,product),rating)
    }.join(model.predict(usersProducts).map{case Rating(user,product,rating)=>((user,product),rating)}).values ;
    math.sqrt(ratingsAndPredictions.map(x=>(x._1-x._2)*(x._1-x._2)).mean())}


  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("Optimize ALS Model Parameters").setMaster("local[2]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("checkpoint")
    sc.setLogLevel("WARN")
//    * 为创建ALS推荐模型寻找最优参数组，需要输入以下参数
//    * trainDataPath: 训练数据的路径
//    * validateDataPath: 训练数据的路径
//    * listRank: 可选的用户项目子矩阵的阶值列表
//    * listIteration: 可选的循环次数值列表
//    * listLambda: 可选的防止过拟合参数值列表
//    * paraOutputPath：最优参数组存储目录
//    * splitter:输入原始数据分隔符
    // 匹配输入参数
    val trainDataPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\trainRatings"
    val validateDataPath ="E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\validateRatings"
    val args2="10,20,30,40,50"
    val args3="10,20"
    val args4 ="0.001,0.01,0.03,0.1,0.3,1,3"
    val listRank = args2.split(",").map(_.toInt)
    val listIteration = args3.split(",").map(_.toInt)
    val listLambda = args4.split(",").map(_.toDouble)
    val paraOutputPath = "E:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第9章\\任务程序\\ALSModelParameters_1"
    val splitter = ","

    // 加载训练集数据
    val trainData = sc.textFile(trainDataPath).map{x=>val fields=x.slice(1,x.size-1).split(splitter);
      (fields(0).toInt,fields(1).toInt,fields(2).toDouble)}
    val trainDataRating= trainData.map(x=>Rating(x._1,x._2,x._3))
    // 加载验证集数据
    val validateData = sc.textFile(validateDataPath).map{x=>val fields=x.slice(1,x.size-1).split(splitter);
      (fields(0).toInt,fields(1).toInt,fields(2).toDouble)}
    val validateDataRating= validateData.map(x=>Rating(x._1,x._2,x._3))
   // 初始化最优参数，取极端值
    var bestRMSE = Double.MaxValue
    var bestRank = -10
    var bestIteration = -10
    var bestLambda = -1.0

    // 参数寻优
    for( rank<- listRank;lambda<-listLambda;iter<-listIteration) {
      val model = ALS.train(trainDataRating,rank,iter,lambda);
      val validationRMSE = computeRMSE(model,validateDataRating);
      if(validationRMSE<bestRMSE){
        bestRMSE=validationRMSE;
        bestRank=rank;
        bestLambda=lambda;
        bestIteration=iter}
        }
    // 输出最优参数组
    println("BestRank:Iteration:BestLambda => BestRMSE")
    println(bestRank + ": " + bestIteration + ": " + bestLambda + " => " + bestRMSE)

    val result = Array(bestRank + "," + bestIteration + "," + bestLambda)
    sc.parallelize(result).repartition(1).saveAsTextFile(paraOutputPath)
    sc.stop()
  }
}
