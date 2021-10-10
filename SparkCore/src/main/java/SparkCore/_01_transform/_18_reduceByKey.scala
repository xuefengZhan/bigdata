package SparkCore._01_transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//todo 1.有shuffle
object _18_reduceByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kv").setMaster("local[1]")
    val sparkContext = new SparkContext(conf)
    val dataRDD1 = sparkContext.makeRDD(List(("a",1),("b",2),("c",3),("a",4)))
    val rdd1 : RDD[(String, Int)] = dataRDD1.reduceByKey(_ + _)
    val rdd2 = dataRDD1.reduceByKey(_+_, 2)


    rdd1.glom().foreach(x=>println(x.mkString(","))) //(a,5),(b,2),(c,3)

    rdd2.glom().foreach(x=>println(x.mkString(",")))
    //(b,2)
    //(a,5),(c,3)
  }


}
