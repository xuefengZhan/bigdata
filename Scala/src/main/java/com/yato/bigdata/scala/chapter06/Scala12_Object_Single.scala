package com.yato.bigdata.scala.chapter06

/**
 * 单例对象
 */
object Scala12_Object_Single {
  def main(args: Array[String]): Unit = {

  }

  //Scala中单例对象
  //todo 1.主构造方法私有化
//  class User private(){
//    def this(name : String) = {
//      this()
//    }
//  }


  //todo 2.Object关键字声明类同时可以声明单例对象`
  // Scala中没有静态操作，采用object关键字直接声明单例对象
  object Person{
    def test() : Unit = {

    }
  }

}
