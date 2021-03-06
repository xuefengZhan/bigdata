package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

object _01_map {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4,5,6,7,8,9,10,11,12,13,14,15,16)
    val src = sc.makeRDD(list,2)
    val map1 = src.map(e => {
      println(">>>>" + e)
      e
    })

    val map2 = map1.map(e => {
      println("####" + e)
      e
    })

    map2.collect().foreach(x=> println(x))
  }
}
