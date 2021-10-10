package com.yato.bigdata.scala.chapter08

/**
 * yatolovefantasy
 * 2021-09-11-11:32
 */
object Scala05_partition {
  def main(args: Array[String]): Unit = {
    //将该List(1,2,3,4,5,6,"test")中的Int类型的元素先加一，再去掉字符串

    //不使用偏函数    缺点是先去掉字符串，然后再加一  改变了需求
    List(1,2,3,4,5,6,"test").filter(_.isInstanceOf[Int]).map(_.asInstanceOf[Int] + 1).foreach(println)

    val list2: List[Any] = List(1, 2, 3, 4, 5, 6, "test").map(
      data => {
        data match {
          case i: Int => i + 1
          case d => d
        }
      }
    ).filter(d => d.isInstanceOf[Int])
    println(list2)  //List(2, 3, 4, 5, 6, 7)


    //使用偏函数
    //collect 只采集符合条件的数据并做处理  这就是偏函数
    List(1, 2, 3, 4, 5, 6, "test").collect { case x: Int => x + 1 }.foreach(println)
  }
}
