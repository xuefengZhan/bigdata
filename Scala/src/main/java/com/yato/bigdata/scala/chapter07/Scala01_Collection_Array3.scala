package com.yato.bigdata.scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-06-21:07
 */
object Scala01_Collection_Array3 {

  def main(args: Array[String]): Unit = {
    //todo 创建数组的第二种方式  使用Array的伴生对象的apply方法创建
    // 至少传入一个元素
    val ints: Array[Int] = Array(1, 2, 3, 4) // 本质上： Array.apply(1,2,3,4)
    println(ints.mkString(","))

    val ints1: Array[Int] = Array(5, 6, 7, 8)

    //todo 数组和数组拼接
    val array = ints ++: ints1
    println(array.mkString(",")) //1,2,3,4,5,6,7,8
  }

}
