package ch8

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TFIDF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input1 = "D:\\新建文件夹\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第8章\\任务程序\\data/"


    val data = sc.textFile(input1+"sample_fpgrowth.txt")
    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))
    val fpg = new FPGrowth().setMinSupport(0.2).setNumPartitions(10)
    val model = fpg.run(transactions)
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent .mkString("[", ",", "]")
          + ", " + rule.confidence)
    }

  }
}
