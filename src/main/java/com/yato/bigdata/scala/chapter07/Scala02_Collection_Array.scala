package com.yato.bigdata.scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Array {
  //todo 可变数组
  val ab : ArrayBuffer[Int] = new ArrayBuffer[Int]()

  ab.append(1, 2, 3, 4)
  ab.appendAll(Array(5,6,7,8))  //特质TraversableOnce的子类  可迭代子类
  ab.insert(2,5,6)//第一个参数是位置
  ab.update(1,0)
  ab.remove(1) //删除指定位置数据
  ab.remove(1,3)//从指定位置删除多少个

}
