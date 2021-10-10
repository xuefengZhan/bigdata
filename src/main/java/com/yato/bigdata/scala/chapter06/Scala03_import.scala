package com.yato.bigdata.scala.chapter06

object Scala03_import {
  def main(args: Array[String]): Unit = {
    //todo 1.import可以放在任意位置
    import java.util.Date
    val value = new Date()


    //todo 2.导入对象
    val user = new User()
    import user._
    println(username)


  }


  class User{
    val username : String = "张三"
  }
}

