package SparkCore._02_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _08_fold {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val list1 = List(1,2,3,4,5,6,7,8,9,10)

    val rdd1: RDD[Int] = sc.makeRDD(list1,4)

    val i = rdd1.fold(10)(_ + _)
    println(i) //105
  }

}
