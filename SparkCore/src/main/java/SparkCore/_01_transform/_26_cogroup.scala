package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object _26_cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.makeRDD(Array((1, "a"), (2, "b"),(2,"d"),(5,"e")))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(Array((1, 4), (2, 5), (3, 6),(4,5),(2,6)))

    val value: RDD[(Int, (Iterable[String], Iterable[Int]))] = rdd.cogroup(rdd1)
    value.collect().foreach(println)
    //(4,(CompactBuffer(),CompactBuffer(5)))
    //(1,(CompactBuffer(a),CompactBuffer(4)))
    //(3,(CompactBuffer(),CompactBuffer(6)))
    //(5,(CompactBuffer(e),CompactBuffer()))
    //(2,(CompactBuffer(b, d),CompactBuffer(5, 6)))
  }
}
