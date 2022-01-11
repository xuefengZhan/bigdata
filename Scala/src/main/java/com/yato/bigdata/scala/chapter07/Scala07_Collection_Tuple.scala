package com.yato.bigdata.scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala07_Collection_Tuple {
  def main(args: Array[String]): Unit = {


    //todo 元组通过顺序号访问元素
    val t = (1,"张三",30)
    println(t._1)
    println(t._2)
    println(t._3)
    //通过索引访问
    println(t.productElement(1))
    //获取元组迭代器
    val iterator: Iterator[Any] = t.productIterator


    //todo Tuple也有类型的概念 TupleX  X是元素个数  最多放置22个
    // 元素可以是各种类型，也可以是集合
    val t1 : (Int,String,Int) = (1,"张三",30)
    //todo 对偶元组   元组数量为2  也叫做对偶元组
  }
}
