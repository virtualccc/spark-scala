package ch8


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.IDF


/**
  * HashingTF计算给定大小的词频向量，通过哈希法排列词向量顺序，使词与向量一一对应
  * IDF计算逆文档频率，需要调用fit()方法来获取一个IDFModel，然后通过IDFModel的transform()把TF转换为IDF
  *
  * 数据分割成单词需要序列化，对每一个单词使用HashingTF将句子转为特征向量，输入类型为RDD[Iterable[]]
  * 使用IDF重新调整向量，得到文档转换后的特征向量
  */
// Load documents (one per line)
object TFIDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TFIDF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input1 = "D:\\新建文件夹\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第8章\\任务程序\\data/"

    val documents= sc.textFile(input1+"tf-idf.txt").map(_.split(" ").toSeq)




    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(documents)
    tf.cache()
    val idf = new IDF().fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)
    tfidf.collect.foreach(println)


    /**
      * result: 每一条第一个值为默认Hash分桶个数，中间的列表的值为单词分配的ID，最后一个列表值对应每一单词逆文档频率
      */

  }
}
