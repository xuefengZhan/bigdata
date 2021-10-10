package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 重新分区
(1)参数1：指定分区个数，分区并行计算，一个分区一个Task
(2)参数2：是否开启shuffle，默认为false
(3)如果不开启shuffle，只可以缩减分区(参数1只能小于原有分区数，如果大于原有分区数不起作用)
(4)并且不会打乱重组，同一个分区的数据还在一个分区，会导致数据倾斜
 */
object _10_coalesce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.makeRDD(List(
      1,2,3,4,5,6
    ),3)

    val dataRDD1 = dataRDD.coalesce(2,true)
    dataRDD1.glom().foreach(x=>println(x.mkString(",")))
    //3,4,5,6
    //1,2
  }
}
