package com.yato.bigdata.scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Method4 {
  def main(args: Array[String]): Unit = {

    val list = List(1,2,3,4)

    // todo 1.集合映射  map  将集合中的每条数据通过一个函数进行转换
    //  函数的返回值不做限制 可有可无
    println("map => " + list.map(x=>{x*2}))
    println("map => " + list.map(x=>x*2))
    println("map => " + list.map(_*2))



    // todo 2.集合扁平化
    //  扁平化就是聚合相反的过程  聚合是多变1  扁平化是1变多
    // 扁平化就是将数组中的非原子性元素 拆分成原子性
    val list1 = List(
      List(1,2),
      List(3,4)
    )
    val l1 : List[Int] = list1.flatten
    println("flatten =>" + l1)  //flatten =>List(1, 2, 3, 4)


    val ab: ArrayBuffer[String] = ArrayBuffer("Hadoop Scala Hive", "Sqoop Flink")
    val abf: ArrayBuffer[Char] = ab.flatten
    println(abf) //ArrayBuffer(H, a, d, o, o, p,  , S, c, a, l, a,  , H, i, v, e, S, q, o, o, p,  , F, l, i, n, k)

    val strings: ArrayBuffer[String] = ab.flatten(str => {
      str.split(" ")  //一变多
    })
    println(strings) //ArrayBuffer(Hadoop, Scala, Hive, Sqoop, Flink)


    // todo 3.集合扁平化映射  带映射的扁平化   可以自定义扁平化规则  和映射规则
    //  将集合中每个元素传递进去进行扁平化（切分成多个数据）   自定义拆分规则
    //  然后再针对每个数据进行映射
    val l2 : List[Int] = list1.flatMap(list => list)
    println("flatMap =>" + l2 ) //flatMap =>List(1, 2, 3, 4)

    ab.flatMap(
      //str 是每个元素 也就是一个字符串
      str=>{
        str.split(" ")
      }
    )
  }
}
