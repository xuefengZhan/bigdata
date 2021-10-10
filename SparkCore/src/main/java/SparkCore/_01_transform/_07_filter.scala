package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

object _07_filter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val src = sc.makeRDD(List(1, 2, 3, 4,5,6),2)

    val dataRDD1 = src.filter(_%2 == 0)

    dataRDD1.glom().foreach(x=>println(x.mkString(",")))  //2    4,6

  }
}
