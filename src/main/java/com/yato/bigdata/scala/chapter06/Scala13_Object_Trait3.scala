package com.yato.bigdata.scala.chapter06

/**
 * 多个特质的混入和使用
 */
object Scala13_Object_Trait3 {
  def main(args: Array[String]): Unit = {
    new User()
  }

  trait A {
    println("aaaa")
  }

  trait B extends A {
    println("bbbbbb")
  }

  trait C {
    println("cccccc")
  }

  class Parent{
    println("pppppppp")
  }

  class User extends Parent with C with B with A {
    println("UUUUUUUUU")
  }
}
