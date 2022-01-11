package com.yato.bigdata.scala.chapter07

object Scala12_Collection_练习 {
  def main(args: Array[String]): Unit = {
    val map1 = Map("a" -> 1, "b" -> 2)
    val map2 = Map("a" -> 3, "c" -> 4)

    // Map("a" -> 4, "b" -> 2,"c" -> 4)
    val stringToInt: Map[String, Int] = map1.foldLeft(map2)(
      (map, t) => {
        map.updated(t._1, map.getOrElse(t._1, 0) + t._2)
      }
    )
    println(stringToInt)  //Map(a -> 4, c -> 4, b -> 2)

  }
}
