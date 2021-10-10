package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _24_join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"), (3, "c"),(2,"d")))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6),(4,5),(2,6)))
    val rdd2 = rdd.join(rdd1, 2)

    val result: RDD[(String, (Int, (String, Int)))] = rdd2.mapPartitionsWithIndex((index, iter) => {
      iter.map(("分区号" + index, _))
    })
    result.collect().foreach(println)
    //(分区号0,(2,(b,5)))
    //(分区号0,(2,(b,6)))
    //(分区号0,(2,(d,5)))
    //(分区号0,(2,(d,6)))
    //(分区号1,(1,(a,4)))
    //(分区号1,(3,(c,6)))
  }
}
