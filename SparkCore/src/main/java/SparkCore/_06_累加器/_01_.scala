package SparkCore._06_累加器

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * yatolovefantasy
 * 2021-10-10-22:18
 */
object _01_ {
  def main(args: Array[String]): Unit = {
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName(" ")
    val sparkContext = new SparkContext(sc)
    val sourceRDD: RDD[Int] = sparkContext.makeRDD(List(1,2,3,4))
    //TODO 1.用累加替换reduce()
    //没有分区内 分区间  直接遍历就完事了
    var sum = 0;
    sourceRDD.foreach(
      num=>{
        sum += num
      }
    )
    println("sum="+sum)
    // todo 执行结果为0
    // 问题解释： foreach是行动算子，是分布式遍历
    // 在executor端执行
    // todo RDD的算子中用到了算子外的变量，因此要闭包
    //
    sparkContext.stop()
  }

}
