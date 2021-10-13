package StructedStreaming._03_流式DF和DS的操作._05_Join

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

/**
 *  streaming df/ds join streaming df/ds
 */

object StreamingStreamingInnerJoinWithoutWaterMark {
  def main(args: Array[String]): Unit = {
    //对 2 个流式数据进行 join 操作. 输出模式仅支持append模式
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("StreamStream1")
      .getOrCreate()

    import spark.implicits._
    // 第 1 个 stream
    val nameSexStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "fantasy")
      .option("port", 9999)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1), Timestamp.valueOf(arr(2)))
      }).toDF("name", "sex", "ts1")


    // 第 2 个 stream
    val nameAgeStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop201")
      .option("port", 20000)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1).toInt, Timestamp.valueOf(arr(2)))
      }).toDF("name", "age", "ts2")


    // join 操作
    val joinResult: DataFrame = nameSexStream.join(nameAgeStream, "name")

    joinResult.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(0))
      .start()
      .awaitTermination()
  }

  //第 1 个数据格式: 姓名,年龄,事件时间
  //lisi,female,2019-09-16 11:50:00
  //zs,male,2019-09-16 11:51:00
  //ww,female,2019-09-16 11:52:00
  //zhiling,female,2019-09-16 11:53:00
  //fengjie,female,2019-09-16 11:54:00
  //yifei,female,2019-09-16 11:55:00


  //第 2 个数据格式: 姓名,性别,事件时间
  //lisi,18,2019-09-16 11:50:00
  //zs,19,2019-09-16 11:51:00
  //ww,20,2019-09-16 11:52:00
  //zhiling,22,2019-09-16 11:53:00
  //yifei,30,2019-09-16 11:54:00
  //fengjie,98,2019-09-16 11:55:00


  //+-------+------+-------------------+---+-------------------+
  //|   name|   sex|                ts1|age|                ts2|
  //+-------+------+-------------------+---+-------------------+
  //|zhiling|female|2019-09-16 11:53:00| 22|2019-09-16 11:53:00|
  //|     ww|female|2019-09-16 11:52:00| 20|2019-09-16 11:52:00|
  //|  yifei|female|2019-09-16 11:55:00| 30|2019-09-16 11:54:00|
  //|     zs|  male|2019-09-16 11:51:00| 19|2019-09-16 11:51:00|
  //|fengjie|female|2019-09-16 11:54:00| 98|2019-09-16 11:55:00|
  //|   lisi|female|2019-09-16 11:50:00| 18|2019-09-16 11:50:00|
  //+-------+------+-------------------+---+-------------------+
}
