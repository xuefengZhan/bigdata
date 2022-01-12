package scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Method1 {
  def main(args: Array[String]): Unit = {
    val buffer: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4)

    //反转
    println(buffer.reverse)

    //取数组中前3条数据
    println(buffer.take(3))

    //取数组中后3条
    println(buffer.takeRight(3))

    //去除数组中第一条数据
    println(buffer.drop(1))

    //从右边丢弃2个
    println(buffer.dropRight(2))

    //获取数组的头 尾
    println(buffer.head) //1
    println(buffer.tail) // 2 3 4
    //集合中不是头的部分就是尾部

    //获取数组最后一个元素
    println(buffer.last) //4
    println(buffer.init)//1 2 3 除了最后一个都是前面的

  }
}
