package scala.chapter07

/**
 * yatolovefantasy
 * 2021-09-09-20:25
 */
object Scala14_WordCount {
  def main(args: Array[String]): Unit = {
    val list = List(
      ("Hello Hive Hadoop Scala",4),
      ("Hello Hive Hadoop",3),
      ("Hello Hive",2),
      ("Hello",1)
    )
    //todo 统计list中每个单词出现的次数
    val result1: List[(String, Int)] = list.flatMap(t => {
      val words: Array[String] = t._1.split(" ")
      words.map(word => (word, t._2))
    })

    //println(result1)
    val stringToTuples: Map[String, List[(String, Int)]] = result1.groupBy(_._1)

    val result: Map[String, Int] = stringToTuples.map(x => {
      val key = x._1
      val value = x._2.foldLeft(0)((m, n) => m + n._2)
      (key, value)
    })

    println(result)
  }

}
