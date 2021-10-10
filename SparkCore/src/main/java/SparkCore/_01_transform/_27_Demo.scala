package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。

//统计出每一个省份每个广告被点击数量排行的Top3
object _27_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo1").setMaster("local")
    val sc = new SparkContext(conf)

    val src: RDD[String] = sc.textFile("I:\\14_Spark\\502_尚硅谷大数据技术之Spark\\2.资料\\data\\agent.log")

    //（(省份，广告)，1）
    val cityAd: RDD[((String, String), Int)] = src.map(line => {
      val words = line.split(" ")
      ((words(1), words(4)),1)
    })
    // (省份，广告)，sum
    val cityAdCnt: RDD[((String,  String), Int)] = cityAd.reduceByKey(_ + _)
    // 省份，(广告，sum)
    val value: RDD[(String, (String, Int))] = cityAdCnt.map(x => {
      (x._1._1, (x._1._2, x._2))
    })

    val provAdCnt: RDD[(String, Iterable[(String, Int)])] = value.groupByKey()

    val result: RDD[(String, List[(String, Int)])] = provAdCnt.mapValues(x => {
      x.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    })

    result.collect().foreach(println)

  }
}
