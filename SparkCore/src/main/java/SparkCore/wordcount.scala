package SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * yatolovefantasy
 * 2021-10-08-21:12
 */
object wordcount {
  def main(args: Array[String]): Unit = {
    //todo 1. 建立和Spark框架的链接
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //todo 2.业务逻辑处理
    val fileRDD: RDD[String] = sc.textFile("SparkCore/target/classes/wc.txt")
    val rdd: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    val tuples: Array[(String, Int)] = rdd.collect()
    tuples.foreach(println)


    //todo 3.关闭链接
    sc.stop()

  }
}
