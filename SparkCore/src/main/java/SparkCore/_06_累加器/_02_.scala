package SparkCore._06_累加器

import org.apache.spark.{SparkConf, SparkContext}

/**
 * yatolovefantasy
 * 2021-10-10-22:36
 */
object _02_ {
  def main(args: Array[String]): Unit = {
    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName(" ")
    val sc  = new SparkContext(conf)
    val rdd = sc.makeRDD(List(1,2,3,4,5))
    //todo 1.获取系统的累加器，Spark默认提供了简单的数据聚合的累加器
    var sum = sc.longAccumulator("sum"); //sum是累加器的名字

    rdd.foreach(
      num => {
        // todo 2.使用累加器
        sum.add(num)
      }
    )
    // todo 3.获取累加器的值
    println("sum = " + sum.value)
  }

}
