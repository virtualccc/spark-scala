package ch8

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object logistictest_project {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("logistic").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input1 = "D:\\新建文件夹\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第8章\\任务程序\\data/"
//
//
//    //数据预处理，csv编程txt，并清洗
//    val data1= sc.textFile(input1+"data.csv")
//
//    val data2=data1.map{x=> val l=x.split(",");val ll = l.map(_.toDouble);ll}
//    val data3 = data2.filter(x=> x.sum!=0).map{x=> val y=x.map(_.toInt);y.mkString(",")}
//
//    data3.repartition(1).saveAsTextFile(input1+"data.txt")

//

    //设置需要的参数
    val inpath = input1+"data.txt" //输入数据路径
    val model_logistic = "E:\\streaming" //模型存储位置
    val f1Score_path = "E:\\streaming/f1Score_path" //F 值输出路径
    val threshold = 0.5 //阈值
    val splitter ="," //数据分隔符
    val bili = 0.8 //训练数据占比
//    所读取数据转化成模型数据并且划分为训练集和测试集
//

    val data = sc.textFile(inpath).map{
      x=> val lines = x.split(splitter);LabeledPoint(lines(0).toDouble,Vectors.dense(lines.slice(1,lines.length).map(_.toDouble)))};
    //分割 training and test
    val splits = data.randomSplit(Array(bili, 1-bili), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    //训练模型
    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training).
      setThreshold(threshold)
    //预测
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    // Precision by threshold
    val precision = metrics.precisionByThreshold
    // Recall by threshold
    val recall = metrics.recallByThreshold
    // F-measure
    val f1Score = metrics.fMeasureByThreshold
//    f1Score.repartition(1).saveAsTextFile(f1Score_path)
    //模型存储
//    model.save(sc,model_logistic)


    println(f1Score.repartition(1))
    println(model)
    //data 文件夹下放模型具体数据
    //存放模型元数据metadata

    /**
      * 使用训练好的模型预测函数
      */
    val sameModel = LogisticRegressionModel.load(sc,model_logistic)
    val predictions = test.map{
      case LabeledPoint(lable,feature) => val prediction = model.predict(feature)
        (feature,prediction)
    }

    /**
      * 评价计算精确率，召回率，F值，ROC曲线
      */

    val metrics1 = new BinaryClassificationMetrics(predictionAndLabels)
    val precision1= metrics1.precisionByThreshold()
    precision1.collect().foreach{case (t,p)=>println(s"Threshold:$t,Precision:$p")}

    val recall1 = metrics1.recallByThreshold()
    recall1.collect().foreach{case (t,r)=>println(s"Threshold:$t,Recall:$r")}

    val f1Score1 = metrics1.fMeasureByThreshold()
    f1Score1.collect().foreach{case (t,f)=>println(s"Threshold:$t,f-score:$f,Beta = 1")}

  }
}


