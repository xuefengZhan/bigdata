package com.yato.bigdata.scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Method {
  def main(args: Array[String]): Unit = {
    val buffer: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4)
    println(buffer.length)

    println(buffer.size)

    println(buffer.isEmpty)

    println(buffer.contains(3))

    val iterator: Iterator[Int] = buffer.iterator
    while(iterator.hasNext){
      println(iterator.next())
    }

    println(buffer.mkString(","))

    buffer.foreach(println)
  }
}
