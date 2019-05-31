package ch6

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streaming {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("streaming").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(5))

    val lines =ssc.textFileStream("E:\\streaming")

    //计算网页热度f =0.1u+0.9x+0.4y+z(u为用户等级，x为用户进入网站到离开网站时间对网页访问次数，y为停留时间，z为是否点赞)
    val html=lines.map{line=>val words=line.split(",");(words(0),0.1*words(1). toInt+0.9*words(2).toInt+0.4*words(3).toDouble+words(4).toInt)}
    //计算每个网页的热度总和
    val htmlCount=html.reduceByKeyAndWindow((v1:Double,v2:Double)=>v1+v2,Seconds (60),Seconds(10))
    //按照网页的热度总和降序排序
    val hottestHtml=htmlCount.transform(itemRDD=>{
      //这里把第二个放前面了，key相当于value
      val top10=itemRDD.map(pair=>(pair._2,pair._1)).sortByKey(false).map (pair=>(pair._2,pair._1)).take(10)
      ssc.sparkContext.makeRDD(top10).repartition(1)
    })

    hottestHtml.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionOfRecords=>{
        val url="jdbc:mysql://192.168.27.128:3306/spark"
        val user="root"
        val password="root"
        val conn=getConn(url,user,password)
        delete(conn,"top_web_page")
        conn.setAutoCommit(false)
        val stmt=conn.createStatement()
        var i=1
        partitionOfRecords.foreach(record=>{
          println("input data is "+record._1+" "+record._2)
          stmt.addBatch("insert into top_web_page(rank,htmlID,pageheat) values ('"+i+"','"+record._1+"','"+record._2+"')")
          i+=1
        })
        stmt.executeBatch()
        conn.commit()
      })
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  //连接对象及清空表方法
  def getConn(url:String,user:String,password:String):Connection={
    Class.forName("com.mysql.jdbc.Driver")
    val conn=DriverManager.getConnection(url,user,password)
    return conn
  }
  def delete(conn:Connection,table:String)={
    val sql="delete from "+table+" where 1=1"
    conn.prepareStatement(sql).executeUpdate()
  }
}
