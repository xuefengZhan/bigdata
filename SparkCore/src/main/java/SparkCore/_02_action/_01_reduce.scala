package SparkCore._02_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _01_reduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    // 聚合数据
    val reduceResult: Int = rdd.reduce(_+_)
    println(reduceResult) //10
  }
}
