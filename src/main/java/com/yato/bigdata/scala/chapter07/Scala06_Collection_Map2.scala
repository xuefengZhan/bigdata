package com.yato.bigdata.scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala06_Collection_Map2 {
  def main(args: Array[String]): Unit = {

    val map = Map(("a", 1), ("Hadoop", 2), ("Spark", 4))

    //k的迭代器
    val iterator: Iterator[String] = map.keysIterator
    val keys: Iterable[String] = map.keys
    val set: Set[String] = map.keySet

    //v的迭代器
    val values: Iterable[Int] = map.values
    val iterator1: Iterator[Int] = map.valuesIterator

    //k-v的迭代器
    val iterator2: Iterator[(String, Int)] = map.iterator
  }
}
