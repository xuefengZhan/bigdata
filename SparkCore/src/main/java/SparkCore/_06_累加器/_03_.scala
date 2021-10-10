package SparkCore._06_累加器

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * yatolovefantasy
 * 2021-10-10-22:46
 */
object _03_ {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(" ")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5))

    var sum = sc.longAccumulator("sum"); //sum是累加器的名字
    val value: RDD[Int] = rdd.map(
      num => {
        // 使用累加器
        sum.add(num)
        num
      }
    )
    // 获取累加器的值
    value.collect()
    value.collect()
    println("sum = " + sum.value)
  }
}
