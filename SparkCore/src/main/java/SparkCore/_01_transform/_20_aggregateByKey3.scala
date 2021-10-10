package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _20_aggregateByKey3 {
//todo 获取相同key的数据的平均值


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val rdd =
      sc.makeRDD(List(
        ("a",1),("a",2),("c",3),
        ("b",4),("c",5),("c",6)
      ),2)

    val value: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (tuple, v) => {
        val sum = tuple._1 + v
        val num = tuple._2 + 1
        (sum, num)
      },
      (p1, p2) => {
        val total = p1._1 + p2._1
        val totalnum = p1._2 + p2._2
        (total, totalnum)
      }
    )
    val result: RDD[(String, Int)] = value.mapValues(tuple => tuple match {
      case (num, cnt) => {
        num / cnt
      }
    })

    result.collect().foreach(x=>println(x))
  }
}
