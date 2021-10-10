package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}
//todo 取并集 ∪  不经过shuffle
object _14_union {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val dataRDD1 = sc.makeRDD(List(1,2,3,4),2)
    val dataRDD2 = sc.makeRDD(List(13,14,15,16),2)
    val dataRDD = dataRDD1.union(dataRDD2)

    dataRDD.glom().foreach(arr=>println(arr.mkString(",")))
    //15,16
    //3,4
    //13,14
    //1,2
  }
}
