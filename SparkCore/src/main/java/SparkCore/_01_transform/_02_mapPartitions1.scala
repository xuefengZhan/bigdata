package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
 //todo 	小功能：获取每个数据分区的最大值
object _02_mapPartitions1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val src = sc.makeRDD(List(1, 2, 3, 4),2)

    val value: RDD[Int] = src.mapPartitions(iter => {
      List(iter.max).iterator
    })
    value.collect().foreach(println)
  }
}
