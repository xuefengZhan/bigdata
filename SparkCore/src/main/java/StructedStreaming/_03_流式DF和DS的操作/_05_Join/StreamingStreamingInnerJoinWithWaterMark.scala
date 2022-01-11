package StructedStreaming._03_流式DF和DS的操作._05_Join

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp

/**
 *  streaming df/ds join streaming df/ds
 */

object StreamingStreamingInnerJoinWithWaterMark {
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
      }).toDF("name1", "sex", "ts1")
      .withWatermark("ts1","2 minutes")

    // 第 2 个 stream
    val nameAgeStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "fantasy")
      .option("port",8888)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1).toInt, Timestamp.valueOf(arr(2)))
      }).toDF("name2", "age", "ts2")
      .withWatermark("ts2","1 minutes")

    //todo 1. inner join 操作
//    val joinResult: DataFrame = nameSexStream.join(nameAgeStream,expr(
//      """
//        | name1 = name2 and
//        | ts2 >= ts1 and
//        | ts2 <= ts1 + interval 1 minutes
//        |""".stripMargin))


    //
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


    //+-------+------+-------------------+-------+---+-------------------+
    //|  name1|   sex|                ts1|  name2|age|                ts2|
    //+-------+------+-------------------+-------+---+-------------------+
    //|zhiling|female|2019-09-16 11:53:00|zhiling| 22|2019-09-16 11:53:00|
    //|     ww|female|2019-09-16 11:52:00|     ww| 20|2019-09-16 11:52:00|
    //|     zs|  male|2019-09-16 11:51:00|     zs| 19|2019-09-16 11:51:00|
    //|fengjie|female|2019-09-16 11:54:00|fengjie| 98|2019-09-16 11:55:00|
    //|   lisi|female|2019-09-16 11:50:00|   lisi| 18|2019-09-16 11:50:00|
    //+-------+------+-------------------+-------+---+-------------------+


    //todo 2. out join  外连接必须使用 watermark   指定joinType即可
    val joinResult: DataFrame = nameSexStream.join(nameAgeStream,expr(
      """
        | name1 = name 2 and
        | ts2 >= ts1 and
        | ts2 <= ts1 + interval 1 minutes
        |""".stripMargin),joinType = "left_join")


    //todo
    joinResult.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(0))
      .start()
      .awaitTermination()
  }

}
