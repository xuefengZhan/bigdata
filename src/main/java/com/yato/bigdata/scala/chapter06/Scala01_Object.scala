package com.yato.bigdata.scala.chapter06

/**
 * yatolovefantasy
 * 2021-09-02-20:48
 */
object Scala01_Object {

  def main(args: Array[String]): Unit = {
    //todo 面向对象编程
    //todo 2.创建对象
    val user : User = new User()
    // todo 3.使用类的属性和方法
    println(user.name)
    user.test()


    println(name)

  }

  // todo 1.声明类
  class User{
    // scala中 类的属性其实就是变量
    val name : String = "zhangsan"
    // scala中 类的方法其实就是函数
    def test() : Unit ={
      println("User...")
    }
  }

}
