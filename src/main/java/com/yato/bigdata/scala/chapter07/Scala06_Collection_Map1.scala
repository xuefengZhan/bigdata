package com.yato.bigdata.scala.chapter07

import scala.collection.mutable

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala06_Collection_Map1 {
  def main(args: Array[String]): Unit = {

    val map = Map(("a", 1), ("Hadoop", 2), ("Spark", 4))

    val v: Option[Int] = map.get("a")
    val v1: Option[Int] = map.get("d")
    println(v) //Some(1)
    println(v1)//None

    //通过key获取value，返回值是个Option对象
    //Option叫做选项，Some为有值  None为无值
    //Option有isEmpty用于判断是否为Nonde的
    //Option的get方法用于取出值
    if(!v1.isEmpty){
      println(v1.get)
    }
    //println(v1.get) //空指针异常

    //Option的getOrElse()方法，如果取不到值，就给予默认值
    println(v1.getOrElse(0))//0

    println(map.getOrElse("d", 0))//0
  }
}
