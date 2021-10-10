package com.yato.bigdata.scala.chapter06

/**
 * yatolovefantasy
 * 2021-09-05-21:00
 */
object Scala08_Object_Method {

  def main(args: Array[String]): Unit = {
      val user = new User();
      user.test()
  }
  class User{
    def test() : Unit = {}
  }
}
