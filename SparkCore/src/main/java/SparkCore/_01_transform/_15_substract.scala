package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

//todo 取差集  必须经过shuffle
object _15_substract {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[4]")
    val sc = new SparkContext(conf)


    val dataRDD1 = sc.makeRDD(List(1,2,3,4))
    val dataRDD2 = sc.makeRDD(List(3,4,5,6))
    val dataRDD = dataRDD1.subtract(dataRDD2)
    dataRDD.glom().foreach(arr=>println(arr.mkString(",")))
    //保留左边的rdd中 右边rdd中没有的数据
    //
    //1
    //
    //2
  }
}
