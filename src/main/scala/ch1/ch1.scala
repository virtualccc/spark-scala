package ch1

object HelloWorld {
  //定义函数，输入a,b以及类型，输出为Int
  def add(a:Int,b:Int):Int = {
    var sum=0;
    sum=a+b;
    return sum
  }
  //匿名函数
  val addInt = (x:Int,y:Int)=> x + y



  def main(args: Array[String]) {

    var z:Array[String] = new Array[String](3);
    z(0)="baidu";z(1)="google"
    //以，打印分割数组,还可以foreach(x=>print(x))
    println(z.mkString(","))

    var arr1=Array(1,2,3)

    arr1=Array(3,1)

    val arr2=Array(4,5,6)
    println(arr2.mkString(","))
    val arr3=arr1++arr2

    // .length   .head   .tail  .isEmpty  .contains
    println(arr3.mkString(","))

    println(HelloWorld.add(2,3))

    println(HelloWorld.addInt(5,7))

    for (i<- 1 to 9;j<- 1 to i){
      print(i+"*"+j+"="+i*j+" ")
      if (i==j) println()
    }
    //Nil表示空，::列表连接 .head 获取第一个 .last  .tail返回所有元素除了第一个
    //take()获取前n个       .contains包含指定元素
    val fruit: List[String] = List("apple","orange","pears")

    //基本使用和List类似，合并是++，添加元素可以是+，元素唯一
    val set:Set[Int]=Set(1,2,3,4,5)

    //  map
    val person :Map[String,Int] = Map("john"->21,"mat"->30)

    println(person.isEmpty)
    println(person.keys)
    println(person.values)

    //元组
    val t= (1,3,"a")

    //map  foreach filter flatMap groupBy
    val testArray:List[Int] = List(1,2,3,4,5)
    val list=List(List(1,2,3),List(3,4,5))

    println(testArray.map(x=>x+1).mkString(","))

    testArray.foreach(x=>print(x*x+"\t"))
    //过滤返归需要的
    testArray.filter(x=>x!=3).foreach(x=>print(x+" "))
    println()
    //把二维压成1维，每个元素乘2打印输出
    list.flatMap(x=>x.map(_*2)).foreach(x=>print(x+" "))
    println()
    //返归分组后的内容
    testArray.groupBy(x=>x%2==0 ).foreach(x=>print(x+" "))

    //scala也可以继承extends,override
    println()
    matchTest(5)

    val test2:List[Int]=List(1,5,8,9,5)
    matchTest1(test2)
    // val 常量 ，var 变量
    val alice = new Person("Alice",12)

    val bob = new Person("Bob",15)

    val poul = new Person("Poul",23)

    for(person1<-List(alice,bob,poul)){
      person1 match{
        case Person("Alice", 12) =>println("hi Alice")
        case Person("Bob", 15) =>println("hi bob")
        case Person(name, age) =>println("name:"+name+"\t"+"age:"+age)
      }
    }




  }

  //模式匹配
  def matchTest(x:Int)=x match{
    case 1 =>println("one")
    case 2 =>println("Two")
    case _ =>println("Three")
  }

  def matchTest1(x:List[Int])=x match{
    case List(0,_,_) =>println("列表x有3个元素并且第一个元素是0")
    case List(1,_*) =>println("列表x有任意个元素并且第一个元素是1")
    case List(_,1,_*)=>println("至少2个元素并且第二个为1")
  }

  //样例类
  case class Person(name:String,age:Int)



}
