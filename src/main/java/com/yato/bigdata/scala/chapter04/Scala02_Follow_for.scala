package com.yato.bigdata.scala.chapter04

class Scala02_Follow_for {

  def main(args: Array[String]): Unit = {
    val range = 1 to 5  // 12345
    val range2 = 1 until 5//1234

    for (elem : Int <- range) {
      println(elem)
    }
    //todo 集合是单类型可以省略数据类型
    for (elem  <- range) {
      println(elem)
    }

    //todo 字符串也是集合
    for(c <- "abc"){
      println(c)
    }
  }

}
