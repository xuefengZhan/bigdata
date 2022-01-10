package scala.chapter07

import scala.collection.mutable.ArrayBuffer

/**
 * yatolovefantasy
 * 2021-09-08-1:22
 */
object Scala03_WordCount {
  def main(args: Array[String]): Unit = {
      val lines: ArrayBuffer[String] = ArrayBuffer(
        "Hello Scala Hive",
        "Hello Scala Hadoop",
        "Spark Flink"
      )

      //将数据拆成一个一个单词  => 扁平化
      val strings: ArrayBuffer[String] = lines.flatMap(str => {
        val strs: Array[String] = str.split(" ")
        strs
      })

      //todo 2.分组 将相同单词放在一起
      val wordToGroup: Map[String, ArrayBuffer[String]] = strings.groupBy(x => x)

      //todo 3.将map 映射成 word count
     // map相当于2个元素的元组，可以通过下划线获取元素
      val result: Map[String, Int] = wordToGroup.map(x => {
        (x._1, x._2.size)
      })

      //打印
      println(result)
      //Map(Hive -> 1, Scala -> 2, Hello -> 2, Spark -> 1, Flink -> 1, Hadoop -> 1)

  }
}
