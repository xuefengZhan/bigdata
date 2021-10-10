package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

object _04_flatMap {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataRDD =sc.makeRDD(List(
      List(1,2),List(3,4)
    ),1)

    val dataRDD1 = dataRDD.flatMap(
      list => list
    )
  }
}
