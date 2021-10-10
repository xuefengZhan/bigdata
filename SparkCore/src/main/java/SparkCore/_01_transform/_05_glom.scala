package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo 	小功能：计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
object _05_glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("glom").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val src = sc.makeRDD(List(1, 2, 3, 4,5,6),2)

    // RDD中每个分区的数据为Array
    val value: RDD[Array[Int]] = src.glom()

    val ints: Array[Int] = value.map(arr => {
      arr.max
    }).collect()

    val sum: Int = ints.sum
    println(sum)
  }
}
