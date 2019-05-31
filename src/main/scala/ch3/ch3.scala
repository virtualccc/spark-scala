package ch3

/**
  * Spark
  * 1.创建RDD
  *     1.从内存创建 parallelize(集合，[分区])
  *     2.外部创建  a.HDFS sc.testFile("") b本地Linux sc.testFile(""file:///)
  * 2.命令
  *     sortBy（排序元素，[true升序default]，[分区]）
  *     collect查询
  *     map(x=>x.spilt(" "))  ---大数组里面包含多个小数组
  *     flatMap(x=>x.spilt(" "))  ----一个数组
  *     take()查询某几个值
  *     rdd1.union(rdd2)合并多个rdd
  *     filter过滤
  *     distinct去重
  *     intersection()交集，subtract()差集，cartesian（）笛卡尔积
  *
  *   k-v
  *     map（x=>(x.spilt(" ")(0),x)）//将x中第一个作为key,x作为value
  *     .value    .keys
  *   reduceByKey :将一个key中前2个value给函数，新return与同key下一个值{对相同KEY相加}
  *     reduceByKey(_+_) / reduceByKey((x,y)=>x+y)
  *   groupByKey
  *     对相同key进行分组，多用于统计个数
  *   join内连接                相同连接
  *   rightOuterJoin            右连接
  *   leftOuterJoin             左连接
  *   fullOuterJoin             全连接
  *
  *   combineByKey
  *      val cb_test= combineByKey(
  *         count => (count,1)//如果键第一次出现为其生成一个累加器，变成元组
  *         (acc:(Int,Int),count)=>(acc._1+count,acc._2+1)//键已经出现过，把值加到对应的累加器中值部分
  *         (acc1:(Int,Int),acc2:(Int,Int))=>  (acc1._1+acc2._1,acc1._2+acc2._1)//如果在不同分区有相同的键，则把对应部分加起来
  *        )
  *      最终结果为（键，（累加值（value值），计数值(出现次数)））
  *      map（x=>(x._1,x._2._1.toDouble/x._2._2)）求平均
  *
  *   zip合并k-v形式RDD，要求分区相同，元素个数相同
  *   .lookup（）查找
  *
  *
  * 3.操作命令
  *     map{//将x按制表符分割，第一、二个元素为String。第三个元素转为Int
  *          x=>val line=x.spilt("\t") ; (line(0),line(1),line(2).toInt)
  *          }
  *
  * 4.文件读取
  *      1.JSON 读
        * import org.json4s._
        * import org.json4s.jackson.JsonMethods._
        * val input = sc.textFile("testjson.json")
        * case class Person(name:String,age:Int)
        * implicit val formats = DefaultFormats
        * val in_json = input.collect.map{x => parse(x).extract[Person]} //接口继承
  *        写
        * import org.json4s.JsonDSL._
        * val json = in_json.map{x=>("name" -> x.name) ~ ("age" -> x.age)}
        * val jsons = json.map{x=>compact(render(x))}
        * sc.parallelize(jsons).repartition(1(1个分区)).saveAsTextFile("json_out")
  *
  *      2.CSV文件读写
        * import java.io.StringReader
        * import au.com.bytecode.opencsv.CSVReader
        * val input = sc.textFile("/tipdm/testcsv.csv")
        * val result = input.map{line=>val reader = new CSVReader(new StringReader(line));reader.readNext();}
        * result.collect
  *
        * import java.io.{StringReader,StringWriter}
        * import au.com.bytecode.opencsv.{CSVReader,CSVWriter}
        * result.map(data => List(data.index,data.title,data.content).toArray).mapPartitions{data=>
        * val stringWriter = new StringWriter();
        * val csvWriter = new CSVWriter(stringWriter);
        * 	csvWriter.writeAll(data.toList)
        * Iterator(stringWriter.toString)}.saveAsTextFile("/tipdm/csv_out")
  *
  *     3.SquenceFile读写
        val output = sc.sequenceFile("/tipdm/outse",classOf[Text],classOf[IntWritable]).map{case (x,y) => (x.toString,y.get())}
        output.collect.foreach(println)

        import org.apache.hadoop.io.{IntWritable,Text}
        val rdd = sc.parallelize(List(("Panda",3),("Kay",6),("Snall",2)))
        rdd.saveAsSequenceFile("/tipdm/outse")

        4.文本
        val bigdata=sc.textFile("result_bigdata.txt")
        val math=sc.textFile("result_math.txt")

        val bigdata=sc.textFile("result_bigdata.txt")
        val math=sc.textFile("result_math.txt")
        val score = bigdata.union(math)
        score.repartition(1).saveAsTextFile("/tipdm/scores")

  *
  *
  */

object ch3 {
  def main(args: Array[String]): Unit = {



  }

}
