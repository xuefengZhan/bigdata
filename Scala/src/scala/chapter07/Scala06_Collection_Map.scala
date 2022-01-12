package scala.chapter07

import scala.collection.mutable

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala06_Collection_Map {
  def main(args: Array[String]): Unit = {

    //todo 1.map中的kv对
    // kv有两种写法：
    //  1. (k,v)
    //  2. k->v
    val map = Map(("a", 1), "Hadoop"-> 2, ("Spark", 4))
    println(map)  //Map(a -> 1, Hadoop -> 2, Spark -> 4)

    //todo 2.map中元素 key重复的话，后者覆盖前者
    val map2 = Map(("a", 1), ("Hadoop", 2), ("Spark", 4),("a",5))
    println(map2)  //Map(a -> 5, Hadoop -> 2, Spark -> 4)

    //todo 3.所有类都有 -> 方法 ，这个方法用于创建关联关系 构成k-v
    val kv1 = "a" -> 1
    val kv2 = kv1 -> 2
    println(kv1) //(a,1)
    println(kv2) //((a,1),2)



    //todo 4.可变Map
    val map3: mutable.Map[String, Int] = mutable.Map("a" -> 1, "b" -> 2, ("c", 3), ("a", 3))
    println(map3) //Map(b -> 2, a -> 3, c -> 3)

    map3.put("d",4)
    map3.update("c",13)
    map3.remove("a")

  }
}
