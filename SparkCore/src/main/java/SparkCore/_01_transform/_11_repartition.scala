package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

object _11_repartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.makeRDD(List(
      1,2,3,4,1,2
    ),2)


    //内部其实执行的是coalesce操作,shuffle 是开启的，不可以关闭
    val dataRDD1 = dataRDD.repartition(4)

    dataRDD1.glom().foreach(arr=>println(arr.mkString(",")));
    //3,4
    //2
    //1
    //1,2
  }
}
