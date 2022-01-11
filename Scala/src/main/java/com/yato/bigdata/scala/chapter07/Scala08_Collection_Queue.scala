package com.yato.bigdata.scala.chapter07

import scala.collection.mutable

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala08_Collection_Queue {
  def main(args: Array[String]): Unit = {

    val que = new mutable.Queue[String]()
    // 添加元素
    que.enqueue("a", "b", "c")
    val que1: mutable.Queue[String] = que += "d"
    println(que eq que1)
    // 获取元素
    println(que.dequeue())
    println(que.dequeue())
    println(que.dequeue())

  }
}
