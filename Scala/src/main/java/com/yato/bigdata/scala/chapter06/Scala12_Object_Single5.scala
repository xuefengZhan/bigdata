package com.yato.bigdata.scala.chapter06

import java.util.Date

/**
 * 单例对象
 */
object Scala12_Object_Single5 {
  def main(args: Array[String]): Unit = {



    val student = new Student()

    println(Student)  //Student$@71c7db30
    println(student)  //Student@19bb089b
    println(Student())//Fri Sep 10 23:41:56 CST 2021
  }

  //半生类
  class Student {
    def test(): Unit = {}
  }

  //伴生对象
  object Student {

    def apply(): Date = new java.util.Date()

    def study(): Unit = {}

  }

}
