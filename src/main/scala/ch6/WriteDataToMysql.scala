package ch6

/** DStream编程模型，离散流编程
  * 窗口长度3s，滑动间隔时间1s   window(Seconds(3),Seconds(1))
  * 使用foreachRDD需要在woker创建连接对象（但这样又会每一个记录创建一个对象，增加开销）
  * 正确方式;
  *       dstream.foreachRDD{rdd=>
  *       rdd.foreachPartition{partitionOfRecords=>
  * val connection = createNewConnection()
  *       partitionOfRecords.foreach(record=> connection.send(record))
  *       connection.close
  * }
  * }
  */
import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.spark.SparkConf
import org.apache.spark.streaming
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
object WriteDataToMysql {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("WriteDataToMySQL").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(10))
    //mini1为连接的linux,先开端口监听后开程序
    val ItemsStream=ssc.socketTextStream("mini1",8888)
    //对端口来的数据进行监听，计数
    val ItemPairs=ItemsStream.map(line=>(line.split(",")(0),1))
    //当前60s窗口内的数据进行合并，必须是batch（一次处理的大小）的整数倍
    val ItemCount=ItemPairs.reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2,Seconds (60),Seconds(10))
    //使用transform后可以任意调用RDD方法
    val hottestWord=ItemCount.transform( itemRDD=>{
      val top3=itemRDD.map(pair=>(pair._2,pair._1)).sortByKey(false).map(pair=> (pair._2,pair._1)).take(3)
      //编程RDD
      ssc.sparkContext.makeRDD(top3)
    })
//    hottestWord.print()
//    写入mql
    hottestWord.foreachRDD(rdd=>{
      rdd.foreachPartition(partitionOfRecords=>{
        val url="jdbc:mysql://192.168.27.128:3306/spark"
        val user="root"
        val password="root"
        Class.forName("com.mysql.jdbc.Driver")
        val conn=DriverManager.getConnection(url,user,password)
        //spark已经对数据对当前时间窗数据合并，写入前需要清空
        conn.prepareStatement("delete from searchKeyWord where 1=1").executeUpdate()
        conn.setAutoCommit(false)
        val stmt=conn.createStatement()
        partitionOfRecords.foreach(record=>{
          stmt.addBatch("insert into searchKeyWord(insert_time,keyword,search_count) values (now(),'"+record._1+"','"+record._2+"')")
        })
        stmt.executeBatch()
        conn.commit()
      })
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
