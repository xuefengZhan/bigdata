package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _22_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local")
    val sc = new SparkContext(conf)

    val list: List[(String, Int)] = List(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))
    val input: RDD[(String, Int)] = sc.makeRDD(list, 2)

    val combineRdd: RDD[(String, (Int, Int))] = input.combineByKey(
      (_, 1),  //对同key的第一个数据进行map转换
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),  //分区内同key的value运算，第一个是value
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  //分区间运算
    )

  }
}
