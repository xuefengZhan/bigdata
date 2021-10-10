package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}


/**
 * 小功能：将List(List(1,2),3,List(4,5))进行扁平化操作
 */
object _04_flatMap_01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataRDD =sc.makeRDD(List(
      List(List(1,2),3,List(4,5))
    ),1)

    val value = dataRDD.flatMap(x => {
      x match {
        case list: List[_] => list
        case dat => List(dat)
      }
    })

    value.collect().foreach(x=>println(x))
    println(List(1))
  }
}
