package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}


//todo 小功能：获取第二个数据分区的数据
object _03_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val src = sc.makeRDD(List(1, 2, 3, 4),2)

    val value = src.mapPartitionsWithIndex((index, iter) => {
      if(index == 1 ) {
        iter
      }else{
        Nil.iterator
      }
    })

    value.collect().foreach(x=>println(x))
  }
}
