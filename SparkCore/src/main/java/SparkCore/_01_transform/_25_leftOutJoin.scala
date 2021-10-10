package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _25_leftOutJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"),(2,"d"),(5,"e")))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6),(4,5),(2,6)))
    rdd.leftOuterJoin(rdd1).collect().foreach(println)

    //(1,(a,Some(4)))
    //(5,(e,None))
    //(2,(b,Some(5)))
    //(2,(b,Some(6)))
    //(2,(d,Some(5)))
    //(2,(d,Some(6)))
  }
}
