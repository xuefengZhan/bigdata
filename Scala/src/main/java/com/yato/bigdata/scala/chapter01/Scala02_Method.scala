package com.yato.bigdata.scala.chapter01

object Scala02_Method {
  def main(args: Array[String]): Unit = {
    //todo 2.调用方法
    test()
    //todo 3.Object声明类和对象，那么也能通过对象来调用
    Scala02_Method.test()
    //todo 4.额外说明：
    // 如果将Scala02_Method看作类的话，那么可以将Scala02_Method.test() 看作是通过类来调用
    // 因此很像Java中的static方法
    // 但是Scala是完全面向对象的，没有static关键字
    // 可以用Object关键字声明对象方法访问方法，来模仿静态语法
  }

  //todo 1.声明一个方法
  def test() : Unit = {
    println("test...")
  }
}
