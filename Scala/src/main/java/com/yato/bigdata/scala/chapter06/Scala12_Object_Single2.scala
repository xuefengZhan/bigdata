package com.yato.bigdata.scala.chapter06

/**
 * 单例对象
 */
object Scala12_Object_Single2 {
  def main(args: Array[String]): Unit = {
    println(User.instance())
    println(User.apply())
    println(User())
    println(User("张三"))
  }

  //半生类
  class User private(){}

  //伴生对象
  object User{
    def instance() = {
      new User()
      //半生类中构造器私有化了
      //但是伴生对象中可以直接new
      //说明伴生对象可以访问半生类中所有内容，包括私有的
    }

    //todo apply方法
    def apply(): User = new User()

    def apply(name : String) : User = new User()
  }

}
