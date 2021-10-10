package SparkCore._02_action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object _11_foreach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val list1 = List(1,2,3,4,5,6,7,8,9,10)


    val rdd1: RDD[Int] = sc.makeRDD(list1,4)

    rdd1.foreach(println)
  }
}
