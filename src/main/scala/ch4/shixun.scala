package ch4

import org.apache.spark.{SparkConf, SparkContext}

object shixun {
  def date_time(date1:String):String={
    val date= date1.trim()
    val hour=date.substring(0,date.indexOf(":")).toInt
    val min=date.substring(date.indexOf(":")+1,date.lastIndexOf(":")).toInt
    if(hour<6 && hour>=23) "深夜"
    else if(hour==6 && min<=30) "深夜"
    else if(hour<11 && hour>=6) "上午"
    else if(hour==11 && min<=30) "上午"
    else if(hour<14 && hour>=11) "中午"
    else if(hour>=14 && hour<17) "下午"
    else if(hour==17 && hour<=30) "下午"
    else if(hour>=17 && hour<19) "傍晚"
    else if(hour==19 && min<=30) "傍晚"
    else "晚上"}

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("shixun").setMaster("local[2]")
    val sc  = new SparkContext(conf)
    val input = "K:\\spark大数据技术与应用\\46488-Spark大数据技术与应用-习题答案.rar\\46488-Spark大数据技术与应用-习题答案\\第4章\\data/jc_content_viewlog.txt"
    val splitter = ","

    val data = sc.textFile(input).map(x=> x.split(splitter))
    //加上collect变成数组才能操作
    val usert50 = data.map(line=>(line(3),1)).reduceByKey(_+_).filter(x=> x._2>50).keys.collect()

    val valib_data=data.filter(x=>usert50.contains(x(3)))
    val web = valib_data.map(x=>(x(2),1)).reduceByKey(_+_).sortBy(x=>x._2,false).take(5)
    web.foreach(x=> println(x))
    //    val userid=data.filter(x=> usert50.contains
//
//    val web= userid.map(x=>(x(3),1)).reduceByKey(_+_).sortBy(x=> x._2,false).keys.take(5)
//    val data2=data.filter(_.length>=6).map{x=>
//      var page=""
//      if(x(2).contains("_")){
//        page=x(2).substring(0,x(2).lastIndexOf("_"))
//       } else{
//        page=x(2)}
//        (x(0),x(1),page,x(3),x(4),x(5))
//      }
//    data2.foreach(x=> println(x))
//    println(web.mkString(","))

//    val time = "    19:29:56"
//    println(date_time(time))




  }
}
