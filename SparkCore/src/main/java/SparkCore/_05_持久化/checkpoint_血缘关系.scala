package SparkCore._05_持久化

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * yatolovefantasy
 * 2021-10-10-20:29
 */
object checkpoint_血缘关系 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //todo 1.设置检查点路径
    sc.setCheckpointDir("checkpoint")

    val list = List("Hello Scala","Hello Spark")

    val rdd =sc.makeRDD(list)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      (word, 1)
    })



    mapRDD.checkpoint()
    //todo 1.在行动算子之前查看血缘关系
    println(mapRDD.toDebugString)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("***************")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)
    //todo 2.在行动算子之后查看血缘关系
    println(mapRDD.toDebugString)

  }

}
