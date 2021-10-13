package SparkStreaming.无状态操作

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 将批次数据转换为RDD进行操作  一个批次调度一次
 */
object _01_Transform {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.196.128", 9999)


    val value: DStream[(String, Int)] = lineDStream.transform(rdd => {
      val wordCount: RDD[(String, Int)] = rdd.flatMap(_.split("")).map((_, 1)).reduceByKey(_ + _)
      wordCount
    })
    //打印
    value.print

    //启动
    ssc.start()
    ssc.awaitTermination()

  }
}
