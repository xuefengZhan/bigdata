package scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Method2 {
  def main(args: Array[String]): Unit = {
    val buffer: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4)

    //todo 1.集合内部的运算
    println(buffer.sum) //10

    println(buffer.max)//4

    println(buffer.min)//1

    println(buffer.product)//积  24

    //todo 2.集合之间的运算
    val buffer1: ArrayBuffer[Int] = ArrayBuffer( 3, 4,5,6)

    //交集
    val  arr1 : ArrayBuffer[Int] = buffer.intersect(buffer1)
    println(arr1)  //ArrayBuffer(3, 4)

    //并集
    val arr2 : ArrayBuffer[Int] = buffer.union(buffer1)
    println(arr2)  //ArrayBuffer(1, 2, 3, 4, 3, 4, 5, 6)

    //差集
    val arr3 : ArrayBuffer[Int] = buffer.diff(buffer1)
    val arr4 : ArrayBuffer[Int] = buffer1.diff(buffer)
    println(arr3) //ArrayBuffer(1, 2)
    println(arr4) //ArrayBuffer(5, 6)

    //去重
    val arr5 : ArrayBuffer[Int] = arr2.distinct
    println(arr5) //ArrayBuffer(1, 2, 3, 4, 5, 6)

  }
}
