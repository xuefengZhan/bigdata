package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

object _06_groupBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val src = sc.makeRDD(List("hadoop","spark","hive","flink"),4)
    val dataRDD1 = src.groupBy(_.charAt(0))

    dataRDD1.collect().foreach(x=>println(x))
    //(h,CompactBuffer(hadoop, hive))
    //(f,CompactBuffer(flink))
    //(s,CompactBuffer(spark))

  }
}
