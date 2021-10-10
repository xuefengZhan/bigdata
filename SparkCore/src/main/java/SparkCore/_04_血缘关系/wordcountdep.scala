package SparkCore._04_血缘关系

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * yatolovefantasy
 * 2021-10-08-21:12
 */
object wordcountdep {
  def main(args: Array[String]): Unit = {
    //todo 1. 建立和Spark框架的链接
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //todo 2.业务逻辑处理
    val fileRDD: RDD[String] = sc.textFile("SparkCore/target/classes/wc.txt")
    println(fileRDD.dependencies)
    println("***********************")

    val words: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(words.dependencies)
    println("***********************")

    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    println(wordToOne.dependencies)
    println("***********************")

    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToSum.dependencies)
    println("***********************")

    val tuples: Array[(String, Int)] = wordToSum.collect()
    tuples.foreach(println)


    //todo 3.关闭链接
    sc.stop()

  }
}
