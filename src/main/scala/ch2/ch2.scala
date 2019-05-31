package ch2

object ch2 {

  def isPalindrom(world: String): Unit ={
    var tmp:String = world.reverse
    if (world == tmp)
      println(world)
  }



  def YHretangle(num:Int):Unit={
    ////构造一个num行num列,int型数组
    val a =Array.ofDim[Int](num,num)
    //i为行row
    for(i<- 0 until a.length){
      a(i)(0)=1
      a(i)(i)=1
    }

    for(i <-2 until a.length ){
      //j为列column
      for(j <-1 until a(i).length){
        a(i)(j)=a(i-1)(j)+a(i-1)(j-1)
      }
    }
    for(i <-0 until a.length){
      //行中的个数
      for(j <- 0 until a(i).length  if j<=i){
        print(a(i)(j)+"\t")
      }
      println()
    }
  }

  def  flower(): Unit ={

    for(i<- 100 until 900){
      var bai = i/100
      var shi = i%100/10
      var ge = i%100%10
      if (ge*ge*ge+shi*shi*shi+bai*bai*bai ==i)
        print(i+"\t")
    }
  }

  def main(args: Array[String]): Unit = {

    val test1 = "pap"
    val test2 = "momf"
    isPalindrom(test2)
    val n:Int= Console.readInt()
    YHretangle(n)
    flower()


  }
}
