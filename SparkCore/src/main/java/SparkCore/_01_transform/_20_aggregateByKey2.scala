package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _20_aggregateByKey2 {
//todo 取出每个分区内相同key的最大值然后分区间相加


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val rdd =
      sc.makeRDD(List(
        ("a",1),("a",2),("c",3),
        ("b",4),("c",5),("c",6)
      ),2)

    // 取出每个分区内相同key的最大值然后分区间相加
    // 1.分区内计算规则：求最大值
    // 2.分区间计算规则：求和
    // 3.初始值：0
    val value: RDD[(String, Int)] = rdd.aggregateByKey(0)(Math.max, _ + _)
    value.collect().foreach(x=>println(x))
  }
}
