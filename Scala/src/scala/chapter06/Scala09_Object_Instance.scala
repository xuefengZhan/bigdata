package scala.chapter06

/**
 * yatolovefantasy
 * 2021-09-05-21:00
 */
object Scala09_Object_Instance {

  def main(args: Array[String]): Unit = {
    //创建对象
    //val user = new User()//调用类的构造方法

    //todo  1.Scala中构造方法的名字不是和类名相同
    //Scala语言万物皆函数，类也是函数

    val user = new User1("张三")

    val user1 = new User1("张三",26)

  }

  //todo 2.类名后面可以增加小括号,小括号代表构造参数列表
  class User(){
    //todo 3.Scala中类的主题内容其实就是类的构造方法体
    // 构建对象的时候，构造方法就会执行，同时类的主体内容也会执行
    println("123")
  }

  //todo 4.多个构造方法的声明，提供类的辅助构造方法
  // Scala中类的构造方法分为2类：主构造方法，  辅助构造方法
  // 主构造方法就是类名后面的参数列表
  // 主要用构造方法创建对象，完成类的初始化操作
  // 辅助构造方法用于辅助主构造方法创建对象 辅助构造方法完成功能补充
  // 辅助构造方法不能独立使用，必须直接或者间接调用主构造方法

  //主构造函数
  class User1(){
    //辅助构造方法:this
    println("xxxxxxx")
    def this(name:String)={
      this() //必须直接或者间接调用主构造方法  this就是主构造方法
      println("yyyyyyyy")
    }

    def this(name:String,age:Int)={
      this(name)
      println("zzzzzzzz")
    }
  }
}
