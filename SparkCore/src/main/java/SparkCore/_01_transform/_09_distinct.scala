package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

object _09_distinct {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.makeRDD(List(
      1,2,3,4,1,2
    ),1)
    val dataRDD1 = dataRDD.distinct()
    dataRDD1.collect().foreach(x=>println(x))
    //4
    //1
    //3
    //2
  }
}
