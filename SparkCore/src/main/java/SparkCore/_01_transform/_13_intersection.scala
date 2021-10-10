package SparkCore._01_transform

import org.apache.spark.{SparkConf, SparkContext}

//todo 取交集 ∩    必须经过shuffle
object _13_intersection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val dataRDD1 = sc.makeRDD(List(1,2,3,4,5,6,7))
    val dataRDD2 = sc.makeRDD(List(3,4,5,6))
    val dataRDD = dataRDD1.intersection(dataRDD2)

    dataRDD.glom().foreach(arr=>println(arr.mkString(",")))
    //3
    //
    //4
    //
    //todo 分区数决定了最后输出的分区个数
    // 做交集运算后，只剩两个元素，会按照hashpartitioner进行分区  分区内没有数据的就为空
  }
}
