package com.yato.bigdata.scala.chapter06

/**
 * yatolovefantasy
 * 2021-09-05-21:33
 */
object Scala10_Object_Extends {
  def main(args: Array[String]): Unit = {
    val user = new Child("张三")
  }


  //当父类提供了有参的构造方法，就不会自动提供无参构造了
  //此时子类必须显式调用父类有参构造
  class Parent(name:String){
    println("111111")

    def this(){
      this("xxxx")
      println("222")
    }
    println("333333333")
  }

  //todo 必须给父类参数表示有参构造
  // 1.指定父类构造参数    class Child extends Parent(name = "张三")
  // 2.子类参数传递给父类构造函数
  class Child(name:String) extends Parent(name){
    println("44444444")

    def this(){
      this("yyyyyyyyy")
      println("55555555555")
    }

    println("66666666")
  }
}
