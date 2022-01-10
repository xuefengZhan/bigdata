package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

object _02_mapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val src = sc.makeRDD(List(1, 2, 3, 4,5,6,7,8,9,10,11,12,13,14,15,16),2)

    val value = src.mapPartitions(
      iter => {
        println(">>>>>>>>>")
        iter.foreach(x=>println(x))
        iter.map(_ * 2)
      }
    )
    value.collect().foreach(x=>println(x))

    //>>>>>>>>>
    //>>>>>>>>>
    //3
    //1
    //4
    //2
  }
}
