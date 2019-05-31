package ch1

import scala.io.Source

object readfile {

  def phonecheck(phonelist:List[String],num:String): Unit ={

    for(line<-phonelist;if line.contains(num))
      println(line)
  }

  def main(args: Array[String]): Unit = {
    val phone = for(line<-Source.fromFile("K:\\spark大数据技术与应用\\46488_Spark大数据技术与应用_源代码和实验数据\\第2章\\任务程序\\data\\2016phonelocation.txt").getLines)
    //      记住每次迭代值保存在数组中，phone为最后这个大数组
      yield line

    var num:String= Console.readLine()
//    115036
    phonecheck(phone.toList,num)

  }
}
