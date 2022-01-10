package scala.chapter07

import scala.collection.mutable.ArrayBuffer


/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Tuple {
  def main(args: Array[String]): Unit = {
    //todo 元组的介绍
    //Scala中，可以将无关的数据作为整体使用，这个整体就是元组
    //元组就相当于没有字段名称的一个表
    //比如 User("张三",28,"男")  这是个Bean，分别为姓名，年纪，性别
    //用元组表示就是：("张三",28,"男") 每个元素没有意义，且元素之间无关

    //todo 元组的声明
    val tuple: (String, Int, String) = ("张三", 28, "男")
    //由于元组中元素之间没有关系且没有名称，只能通过位置访问元组中的元素
    println(tuple._1)  //张三
    println(tuple._2)  //28
    println(tuple._3)  //男

    //todo Scala中键值对对象就是元组  k->v
    // 将只有两个元素的tuple称之为键值对
    // 也称之为对偶元组

  }
}
