package ch8


import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object bayes {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TFIDF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input1 = "D:\\新建文件夹\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第8章\\任务程序\\data/"

    val data= sc.textFile(input1+"sample_naive_bayes_data.txt")
    val parsedData = data.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))}
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")//multinomial 或 bernoulli
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() /test.count()




  }
}
