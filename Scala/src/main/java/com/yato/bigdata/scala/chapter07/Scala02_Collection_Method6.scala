package com.yato.bigdata.scala.chapter07

import scala.collection.mutable.ArrayBuffer


/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Method6 {
  def main(args: Array[String]): Unit = {

    val buffer: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4)

    // todo 集合排序  参数是个函数，函数返回值表示排序规则
    // 如果返回值是字符串，按照字典顺序
    // 数字
    // 默认升序
    println("sortBy =>" + buffer.sortBy(num=>num))

    // 降序 Int.reverse 用int的反向排序规则
    println("sortBy =>" + buffer.sortBy(num=>num)(Ordering.Int.reverse))



    println("sortWith =>" + buffer.sortWith((left, right) => {left < right}))



  }
}
