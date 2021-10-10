package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


//todo def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.length) : RDD[(K, V)]
// 全局排序,所以必然有shuffle
// 根据key来排序的， key必须实现Ordered接口(特质)
object _23_SortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local")
    val sc = new SparkContext(conf)

    val dataRDD1 = sc.makeRDD(List(("a",1),("b",2),("c",3),("a",4),("a",5)),4)
    val sortRDD1: RDD[(String, Int)] = dataRDD1.sortByKey(true,2)
    val sortRDD2: RDD[(String, Int)] = dataRDD1.sortByKey(false)

    val tuples: Array[(String, Int)] = sortRDD1.collect()
    tuples.foreach(println)

    val value: RDD[(Int, (String, Int))] = sortRDD1.mapPartitionsWithIndex((index, iter) => {
      iter.map(x => {
        (index, x)
      })
    })
    value.collect().foreach(println)
  }
}
