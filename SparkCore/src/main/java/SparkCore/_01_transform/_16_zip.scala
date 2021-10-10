package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}


//todo 将两个RDD中的元素，按照位置拉在一起，形成tuple
object _16_zip {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val dataRDD1 = sc.makeRDD(List(1,2,3,4))
    val dataRDD2 = sc.makeRDD(List(3,4,5,6))
    val dataRDD = dataRDD1.zip(dataRDD2)
    dataRDD.glom().foreach(arr=>println(arr.mkString(",")))
    //两个分区：
    //(1,3),(2,4)
    //(3,5),(4,6)

    //(3)没有shuffle
    //(4)两个RDD必须分区数一致 并且 每个分区内元素个数必须一致，要不然元素对不上
    //(5)两个RDD数据类型不一致没问题
    //(6)两个RDD数据分区不一致(Error)
    //(7)两个RDD分区数据数量不一致(Error)

  }
}
