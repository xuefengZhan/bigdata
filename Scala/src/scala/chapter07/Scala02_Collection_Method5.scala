package scala.chapter07

import scala.collection.mutable.ArrayBuffer


/**
 * yatolovefantasy
 * 2021-09-06-22:20
 */
object Scala02_Collection_Method5 {
  def main(args: Array[String]): Unit = {

    val buffer: ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4)
    //TODO 1.功能函数 filter  参数是个返回值为boolean 类型的函数
    // 保留为true的
    val abfiter: ArrayBuffer[Int] = buffer.filter(_ % 2 == 0)
    println("filter =>" + abfiter)  //filter => ArrayBuffer(2, 4)

    //TODO 2.根据制定规则 对数据集中数据进行分组
    // 参数中的函数 的返回值就是分组的key
    // groupBy 后是个Map，key是分组key value是数据集的子集
    val abgro : Map[Int, ArrayBuffer[Int]] = buffer.groupBy(_ % 2)
    println("groupBy =>" +  abgro)  //groupBy =>Map(1 -> ArrayBuffer(1, 3), 0 -> ArrayBuffer(2, 4))

    val strings: ArrayBuffer[String] = ArrayBuffer("Hadoop", "Hive", "Spark", "Sqoop", "Flume")
    val stringToStrings: Map[String, ArrayBuffer[String]] = strings.groupBy(_.substring(0, 1))
    println(stringToStrings) // Map(S -> ArrayBuffer(Spark, Sqoop), F -> ArrayBuffer(Flume), H -> ArrayBuffer(Hadoop, Hive))

  }
}
