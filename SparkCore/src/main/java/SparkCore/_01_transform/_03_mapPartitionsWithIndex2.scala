package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}


//todo 小功能：获取数据的分区号
object _03_mapPartitionsWithIndex2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val src = sc.makeRDD(List(1, 2, 3, 4),4)

    val value = src.mapPartitionsWithIndex((index, iter) => {
      iter.map(x=>(index,x))
    })

    value.collect().foreach(x=>println(x))
    //(0,1)
    //(1,2)
    //(2,3)
    //(3,4)
  }
}
