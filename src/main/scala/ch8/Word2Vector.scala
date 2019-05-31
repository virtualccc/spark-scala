package ch8


import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._

object Word2Vector {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TFIDF").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input1 = "D:\\新建文件夹\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第8章\\任务程序\\data/"


    val input = sc.textFile(input1+"w2v").map(_.split(" ").toSeq)
    val word2vec = new Word2Vec()
    val model = word2vec.fit(input)
    //寻找与“I”语义相同的 10 个词，输出与“I”相似的词以及相似度
    val synonyms = model.findSynonyms("I",10)
    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }
  }
}
