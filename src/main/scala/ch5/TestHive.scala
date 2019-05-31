package ch5

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
/**
  * 加上hive-site.xml,并修改参数才能同步
  */
object TestHive {
  def main(args: Array[String]): Unit = {


    val conf= new SparkConf().setAppName("TestHive").setMaster("local[2]")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val df=spark.sql("show databases")
    val dft=spark.sql("show tables")
//    spark.sql("drop database test2")
//    spark.sql("show databases").collect().foreach(x=>println(x))
//    spark.sql("create database law")
    spark.sql("use law")
//    spark.sql("CREATE TABLE  law ( " +
//      "ip bigint, area int,ie_proxy string, ie_type string ,userid string,clientid string,time_stamp bigint," +
//      "time_format string,pagepath string,ymd int,visiturl string,page_type string,host string,page_title string," +
//      "page_title_type int,page_title_name string,title_keyword string,in_port string,in_url string," +
//      "search_keyword string,source string)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
//      "STORED AS TEXTFILE")
//    spark.sql("use law")
//    spark.sql("show tables").collect().foreach(x=>println(x))
val pageType=spark.sql("select substring(page_type,1,3) as page_type,count(*) as count_num,round((count(*)/837450.0)*100,4) as weights from law group by substring(page_type,1,3)")

    pageType.orderBy(-pageType("count_num")).show()

    pageType.repartition(1).write.mode(SaveMode.Overwrite).json("hdfs://mini1:9000/user/root/sparkSql/pageType.json")


//    dft.show
    spark.stop()
  }

}

/**
  * 1.任务5.3创建law表并导入数据
  * create database law;
  * use law;
  * CREATE TABLE  law (
  * ip bigint,
  * area int,
  * ie_proxy string,
  * ie_type string ,
  * userid string,
  * clientid string,
  * time_stamp bigint,
  * time_format string,
  * pagepath string,
  * ymd int,
  * visiturl string,
  * page_type string,
  * host string,
  * page_title string,
  * page_title_type int,
  * page_title_name string,
  * title_keyword string,
  * in_port string,
  * in_url string,
  * search_keyword string,
  * source string)
  * ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  * STORED AS TEXTFILE;
  * load data inpath '/user/root/law_utf8.csv' overwrite into table law;
  *
  *
  * 2.网页类型分析
  * （1）网页类型统计
  * import org.apache.spark.sql.SaveMode
  *
  * //截取子串
  * val pageType=hiveContext.sql("select substring(page_type,1,3) as page_type,count(*) as count_num,round((count(*)/837450.0)*100,4) as weights from lawgroup by substring(page_type,1,3)")
  *
  * //orderBy(-)默认升序，加-号为降序
  * pageType.orderBy(-pageType("count_num")).show()
  * pageType.repartition(1).save("/user/root/sparkSql/pageType.json","json",SaveMode.Overwrite)
  * （2）网页类别统计
  * val pageLevel=hiveContext.sql("select substring(page_type,1,7) as page_type,count(*) as count_num from law where visiturllike '%faguizt%' and substring(page_type,1,7) like '%199%' group by page_type")
  * pageLevel.show()
  * pageLevel.repartition(1).save("/user/root/sparkSql/pageLevel.json","json",SaveMode.Overwrite)
  * （3）咨询内部统计
  * val consultCount=hiveContext.sql("select substring(page_type,1,6) as page_type,count(*) as count_num,round((count(*)/411665.0)*100,4) as weights from law where substring(page_type,1,3)=101 group by substring(page_type,1,6)")
  * consultCount.orderBy(-consultCount("count_num")).show()
  * consultCount.repartition(1).save("/user/root/sparkSql/consultCount.json","json",SaveMode.Overwrite)
  * （4）网页中带有“?”的记录统计
  * hiveContext.sql("select count(*) as num from law where visiturllike '%?%'").show()
  * val pageWith=hiveContext.sql("select substring(page_type,1,7) as page_type,count(*) as count_num,round((count(*)*100)/65477.0,4) as weights from law where visiturllike '%?%' group by substring(page_type,1,7)")
  * pageWith.orderBy(-pageWith("weights")).show()
  * pageWith.repartition(1).save("/user/root/sparkSql/pageWith.json","json",SaveMode.Overwrite)
  * （5）分析其他类型网页的内部规律
  * val otherPage=hiveContext.sql("select count(*) as count_num,round((count(*)/64691.0)*100,4) as weights,page_title from law where visiturllike '%?%' and substring(page_type,1,7)=1999001 group by page_title")
  * otherPage.orderBy(-otherPage("count_num")).limit(5).show()
  * otherPage.orderBy(-otherPage("count_num")).limit(5).save("/user/root/ sparkSql/otherPage.json","json",SaveMode.Overwrite)
  * （6）统计“瞎逛用户”点击的网页类型
  * val streel=hiveContext.sql("select count(*) as count_num,substring(page_ type,1,3) as page_type from law where visiturl not like '%.html' group by substring(page_type,1,3)")
  * streel.orderBy(-streel("count_num")).limit(6).show()
  * streel.orderBy(-streel("count_num")).limit(6).save("/user/root/sparkSql/streel.json","json",SaveMode.Overwrite)
  * 3.点击次数分析
  * （1）用户点击次数统计
  * hiveContext.sql("select count(distinct userid) from law").show()
  * val clickCount=hiveContext.sql("select click_num,count(click_num) as count,round(count(click_num)*100/350090.0,2),round((count(click_num)*click_num)*100/837450.0,2) from (select count(userid) as click_num from lawgroup by userid)tmp_table group by click_num order by count desc")
  * clickCount.limit(7).show()
  * clickCount.limit(7).save("/user/root/sparkSql/clickCount.json","json",SaveMode.Overwrite)
  * （2）浏览一次用户统计分析
  * val onceScan=hiveContext.sql("select page_type,count(page_type) as count,round((count(page_type)*100)/229365.0,4) from (select substring(a.page_ type,1,7) as page_type from law a,(select userid from lawgroup by userid having(count(userid)=1))b where a.userid=b.userid)c group by page_type order by count desc")
  * onceScan.limit(5).show()
  * onceScan.limit(5).save("/user/root/sparkSql/onceScan.json","json",SaveMode.Overwrite)
  * （3）统计点击一次用户访问URL排名
  * val urlRank=hiveContext.sql("select a.visiturl,count(*) as count from law a,(select userid from lawgroup by userid having(count(userid)=1))b where a.userid=b.userid group by a.visiturl")
  * urlRank.orderBy(-urlRank("count")).limit(7).show(false)
  * urlRank.orderBy(-urlRank("count")).limit(7).save("/user/root/sparkSql/urlRank.json","json",SaveMode.Overwrite)
  * （4）原始数据中包含以.html扩展名的网页点击率统计
  * val clickHtml=hiveContext.sql("select a.visiturl,count(*) as count from law a where a.visiturllike '%.html%' group by a.visiturl")
  * clickHtml.orderBy(-clickHtml("count")).limit(10).show(false)
  * clickHtml.orderBy(-clickHtml("count")).limit(10).save("/user/root/sparkSql/clickHtml.json","json",SaveMode.Overwrite)
  * （5）翻页网页统计
  * hiveContext.sql("select count(*)  from law where visiturl like 'http://www.%.cn/info/gongsi/slbgzcdj/201312312876742.html'").show()
  * hiveContext.sql("select count(*)  from law where visiturl like 'http://www.%.cn/info/gongsi/slbgzcdj/201312312876742_2.html'").show()
  * hiveContext.sql("select count(*)  from law where visiturl like 'http://www.%.cn/info/hetong/ldht/201311152872128.html'").show()
  * hiveContext.sql("select count(*)  from law where visiturl like 'http://www.%.cn/info/hetong/ldht/201311152872128_2.html'").show()
  * hiveContext.sql("select count(*)  from law where visiturl like 'http://www.%.cn/info/hetong/ldht/201311152872128_3.html'").show()
  * hiveContext.sql("select count(*)  from law where visiturl like 'http://www.%.cn/info/hetong/ldht/201311152872128_4.html'").show()
  */
