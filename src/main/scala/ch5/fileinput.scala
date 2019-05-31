package ch5


/**创建DATAFRAME
  *
  * 结构化数据文件，在HDFS上的
  * Parquet
  * sqlContext.read.loac("')
  *
  * JSON
  * sqlContext.read.format("json").load("")
  *
  * 外部数据库 MYSQL,ORACLE等
  * val url ="jdbc:mysql://mini1/test"
  * val jdbcDF=sqlContext.read.format("jdbc").option(
  *   Map("url"->url,
  *       "user"->"root"
  *       "password"->"root"
  *       "dbtable"->"people"
  *   )
  * ).load()
  *
  * RDD创建dataFrame
  *
  * case class Person(name:String,age:Int)
  * val data=sc.textfile("").map(_.spilt(""))
  * val people = data.map(p=>Person(p(0),p(1).trim.toInt)).toDF()
  *
  * HIVE表
  *
  * 声明HiveContext对象
  * hiveContext.sql("use test")
  * hiveContext.sql("select * from people")
  *
  * -----------------------------------------
  * 查看数据模式
  * .printSchema
  * 查看数据
  * .show(n)
  *
  * .first()
  * .take/.head(n)前n行
  * takeAsList(n)前n行以list展示
  * .collect获取所有返回Array
  * .collectAsList
  *
  * where
  * select
  * selectExpr对指定字段特殊操作
  * sqlContext.udf.register("replace",(x:String)=>{
  *     x match {
  *         case "M" => 0
  *          case "F" => 1
  *     }
  *   })
  *
  *   user.selectExpr("userId","replace(gender) as sex","age")
  * col/apply,获取一个字段
  *
  *
  * 保存
  * 1
  * val saveoption=Map("header"->"true","path"->"")
  * val data= user.select("","","")
  * import ....sql.savemoede
  * data.write.format("json").mode(SaveMode.Overwrite).options(saveoption).save*
  *
  *
  * 2
  * people.save("","json",SaveMode.Overwrite)
  *
  * 保存成一张表
  * data.saveAsTable("表名"，SaveMode.Overwrite)
  *
  */

object fileinput {

}
