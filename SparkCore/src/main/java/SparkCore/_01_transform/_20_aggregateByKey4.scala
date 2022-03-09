package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _20_aggregateByKey4 {
//todo 获取相同key的数据的平均值


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val rdd = sc.makeRDD(List(
        ("a",1),("a",2),("c",3),
        ("b",4),("c",5),("c",6)
      ),2)

    // 平均值 =  value总和 / key个数
    val value : RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (v1: (Int, Int), v2) => {
        (v1._1 + v2, v1._2 + 1)
      },
      (v3: (Int, Int), v4: (Int, Int)) => {
        (v3._1 + v4._1, v3._2 + v4._2)
      }
    )

    val result = value.mapValues(x => {
      x._1 / x._2
    })

     result.collect().foreach(x=>println(x))
  }
}
