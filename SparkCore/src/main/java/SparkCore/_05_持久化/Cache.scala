package SparkCore._05_持久化

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * yatolovefantasy
 * 2021-10-10-20:10
 */
object Cache {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("wc").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val list = List("Hello Scala","Hello Spark")

    val rdd =sc.makeRDD(list)

    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = flatRDD.map(word => {
      println("###########")
      (word, 1)
    })

    //mapRDD.cache()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("***************")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)


  }
}
