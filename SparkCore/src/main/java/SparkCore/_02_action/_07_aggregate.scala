package SparkCore._02_action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object _07_aggregate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val list1 = List(1,2,3,4,5,6,7,8,9,10)


    val rdd1: RDD[Int] = sc.makeRDD(list1,4)
    val value = rdd1.mapPartitionsWithIndex((index, iter) => {
      iter.map(x => {
        (index, x)
      })
    })
    value.collect().foreach(println)
    //(0,1)
    //(0,2)
    //(1,3)
    //(1,4)
    //(1,5)
    //(2,6)
    //(2,7)
    //(3,8)
    //(3,9)
    //(3,10)

    val str: String = rdd1.aggregate("+")(_ + _, _ + _)
    println(str)
    //++345+12+8910+67

    //todo aggregate
    // zeroValue : 参与一次分组内计算，做初始值
    //             参与一次分组间计算，做初始值

    //todo aggregateByKey 是行动算子
    // zeroValue: 只参与一次分组内计算，做初始值
  }
}
