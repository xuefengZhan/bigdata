package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。

//统计出每一个省份  点击数量排行 Top3的广告

object _27_Demo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo1").setMaster("local")
    val sc = new SparkContext(conf)

    val src: RDD[String] = sc.textFile("E:\\work\\bigdata\\SparkCore\\src\\main\\resources\\agent.log")


    val value = src.map(x => {
      val fields = x.split(" ")
      val province = fields(1);
      val ad = fields(4);

      ( (province, ad ), 1)
    })

    val value1: RDD[((String, String), Int)] = value.reduceByKey(_ + _)

    //省份，(广告，点击数)
    val value2 = value1.map(x => {
      (x._1._1, (x._1._2, x._2))
    })

    val value3: RDD[(String, Iterable[(String, Int)])] = value2.groupByKey()


    val result: RDD[(String, List[(String, Int)])] = value3.mapValues(iter => {
      iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })

    result.collect().foreach(println)


  }
}
