package StructedStreaming._03_流式DF和DS的操作._05_Join

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 *  streaming df/ds 和 静态的ds/df   join
 */
object StreamingStatic2 {
  def main(args: Array[String]): Unit = {
    //1.SparkSession的配置
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.streaming.checkpointLocation","/tmp/spark/ChainBigScreenRealTimeOrder")
      .set("spark.sql.crossJoin.enabled","true")
      .setMaster("local[*]")

    val spark = SparkSession.builder().appName("join").config(conf).getOrCreate()




  }
}
