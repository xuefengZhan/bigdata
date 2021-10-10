package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

object _12_sortBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.makeRDD(List(
      1,2,3,4,5,6),3)

    //todo parm1:排序的key
    //todo parm2:升序降序
    //todo parm3:排序后重新分区的分区数
    val dataRDD1 = dataRDD.sortBy(num => num, false, 2)
    dataRDD1.glom().foreach(arr=>println(arr.mkString(",")))
    //分区数为1 ：6,5,4,3,2,1

    //分区数为2：
    //3,2,1
    //6,5,4

    //分区数为3：
    //6,5
    //4,3
    //2,1

    //分区数为4：
    //6
    //5,4
    //3
    //2,1


  }
}
