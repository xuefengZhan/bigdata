package com.yato.bigdata.scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Method3 {
  def main(args: Array[String]): Unit = {

    //todo 集合之间的运算
    val buffer: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4)
    val buffer1: ArrayBuffer[Int] = ArrayBuffer(3, 4,5)

    //1.按照位置进行拉链  向短的对齐
    val tuples: ArrayBuffer[(Int, Int)] = buffer.zip(buffer1)
    println(tuples)   //ArrayBuffer((1,3), (2,4), (3,5))

    //2.滑窗
    val iterator: Iterator[ArrayBuffer[Int]] = buffer.sliding(3)
    while(iterator.hasNext){
      val next: ArrayBuffer[Int] = iterator.next
      println(next.sum)
    }
    // 6  9

    val iterator1: Iterator[ArrayBuffer[Int]] = buffer.sliding(3, 3)
    while(iterator1.hasNext){
      val next: ArrayBuffer[Int] = iterator1.next
      println(next.sum)
    }
    // 6  4

  }
}
