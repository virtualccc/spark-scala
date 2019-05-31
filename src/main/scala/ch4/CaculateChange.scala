package ch4

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object CaculateChange {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName("CaculateChange").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val input = "K:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第4章\\任务程序\\data\\reform_data/000002.csv"
    val output = "K:\\res"
    //定义文本分隔符
    val splitter = ","
//    date	open	high	close	low	volume	price_change	p_change	ma5	ma10	ma20	v_ma5	v_ma10	v_ma20	turnover	halt
//      2013/10/21	9.18	9.2	9.18	9.04	668149.25	0.01	0.11	9.18	9.18	9.18	668149.25	668149.25	668149.25	0.69	0


    val fm=new SimpleDateFormat("yyyy-MM-dd")
    //这个filter过滤第一行名字
    val data = sc.textFile(input).filter(x => ! x.contains("date"))

    val data1=data.map{x=>
      val split_data = x.split(splitter)
      //日期转为时间戳
      val timestamp = fm.parse(split_data(0))
      println(timestamp)
      println(timestamp.getTime())
      //时间戳，（日期，收盘价）
      (timestamp.getTime(),(split_data(0),split_data(3).toDouble))
      //
    }.filter(x=>x._2._2!=0).sortByKey().map(x=>(x._2._1,x._2._2))
    val queue = new mutable.Queue[Double]()
//    （日期，收盘价）
    val zhangfu = data1.map { x =>
      queue.enqueue(x._2);
      if (queue.size == 2) {
        val cha = (queue.last - queue.head) / queue.head;
//        出队
        queue.dequeue();
        (x._1, cha)
      }
      else {
        (0, 0)
      }
    }
    //过滤最初开始比较的
    val result = zhangfu.filter(x=>x._1!=0).map(x=>x._1+","+x._2)

    result.foreach(x=>println(x))

//    result.repartition(1).saveAsTextFile(output)
  }
}
