package com.yato.bigdata.scala.chapter10

/**
 * yatolovefantasy
 * 2021-09-11-13:30
 */
object Scala01_ScalaImplicit {
  def main(args: Array[String]): Unit = {
    implicit def transform( d : Double ): Int = {
      d.toInt
    }
    var d : Double = 2.0
    val i : Int = d
    println(i)
  }

}
