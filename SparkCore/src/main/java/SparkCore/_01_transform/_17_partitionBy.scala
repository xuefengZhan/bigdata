package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object _17_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd : RDD[(Int, String)] = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    val rdd2 = rdd.partitionBy(new HashPartitioner(2))

    //如果key为null，分区号为0,key 不为null 则获取key的hash值，和分区数取模
    //如果partitionBy中的分区器和当前分区器一样，而且分区数量也一样，就什么都不会做。
    //如果分区器不同或者分区数不同，就会重新按照分区器规则分区

  }
}
