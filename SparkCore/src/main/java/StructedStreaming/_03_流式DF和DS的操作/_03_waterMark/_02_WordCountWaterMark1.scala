package StructedStreaming._03_流式DF和DS的操作._03_waterMark

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.sql.Timestamp

/**
 * todo. update 输出模式下 waterMark的效果演示
 */
object _02_WordCountWaterMark1 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCountWatermark1")
      .getOrCreate()

    import spark.implicits._
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "192.168.196.128")
      .option("port", 9999)
      .load

    // 输入的数据中包含时间戳, 而不是自动添加的时间戳
    val words: DataFrame = lines.as[String].flatMap(line => {
      val split = line.split(",")
      split(1).split(" ").map((_, Timestamp.valueOf(split(0))))
    }).toDF("word", "timestamp")

    import org.apache.spark.sql.functions._


    val wordCounts: Dataset[Row] = words
      .withWatermark("timestamp", "2 minutes")
      .groupBy(window($"timestamp", "10 minutes", "2 minutes"), $"word")
      .count()

    val query: StreamingQuery = wordCounts.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .option("truncate", "false")
      .start
    query.awaitTermination()

    //1.输入数据:2019-08-14 10:55:00,dog
    //(1)当前waterMark = 0
    //(2)计算所有窗口,将未过期的窗口并且窗口的endTime > waterMark的 并且 在 update 模式下,只输出结果表中涉及更新或新增的窗口
    //+------------------------------------------+----+-----+
    //|window                                    |word|count|
    //+------------------------------------------+----+-----+
    //|[2019-08-14 10:46:00, 2019-08-14 10:56:00]|dog |1    |
    //|[2019-08-14 10:52:00, 2019-08-14 11:02:00]|dog |1    |
    //|[2019-08-14 10:50:00, 2019-08-14 11:00:00]|dog |1    |
    //|[2019-08-14 10:48:00, 2019-08-14 10:58:00]|dog |1    |
    //|[2019-08-14 10:54:00, 2019-08-14 11:04:00]|dog |1    |
    //+------------------------------------------+----+-----+
    //(3) 然后根据当前批次中最大的 event-time, 计算出来下次使用的 watermark.
    // 本批次只有一个数据(10:55), 所有: watermark = 10:55 - 2min = 10:53


    // 2. 输入数据:2019-08-14 11:00:00,dog
    //(1)当前waterMark = 10:53
    //(2)计算所有窗口,将未过期的窗口并且窗口的endTime > waterMark的 并且 在 update 模式下,只输出结果表中涉及更新或新增的窗口
    //+------------------------------------------+----+-----+
    //|window                                    |word|count|
    //+------------------------------------------+----+-----+
    //|[2019-08-14 11:00:00, 2019-08-14 11:10:00]|dog |1    |
    //|[2019-08-14 10:52:00, 2019-08-14 11:02:00]|dog |2    |
    //|[2019-08-14 10:58:00, 2019-08-14 11:08:00]|dog |1    |
    //|[2019-08-14 10:54:00, 2019-08-14 11:04:00]|dog |2    |
    //|[2019-08-14 10:56:00, 2019-08-14 11:06:00]|dog |1    |
    //+------------------------------------------+----+-----+
    //其中: count 是 2 的表示更新, count 是 1 的表示新增. 没有变化的就没有显示.(但是内存中仍然保存着)
    //(3) 然后根据当前批次中最大的 event-time, 计算出来下次使用的 watermark. 10:58


    //3.输入数据:2019-08-14 10:55:00,dog
    //(1) 此数据对应的五个窗口中，当前内存中有两个窗口的结束时间已经低于 10: 58.
    //|[2019-08-14 10:48:00, 2019-08-14 10:58:00]|dog |1    |
    //|[2019-08-14 10:46:00, 2019-08-14 10:56:00]|dog |1    |
    //(2) 则立即删除这两个窗口在内存中的维护状态. 同时, 当前批次中新加入的数据所划分出来的窗口, 如果窗口结束时间低于 10:58, 则窗口会被过滤掉.
    //所以这次输出结果:
    //+------------------------------------------+----+-----+
    //|window                                    |word|count|
    //+------------------------------------------+----+-----+
    //|[2019-08-14 10:52:00, 2019-08-14 11:02:00]|dog |3    |
    //|[2019-08-14 10:50:00, 2019-08-14 11:00:00]|dog |2    |
    //|[2019-08-14 10:54:00, 2019-08-14 11:04:00]|dog |3    |
    //+------------------------------------------+----+-----+
    // 第三个批次的数据处理完成后, 立即计算: watermark= 10:55 - 2min = 10:53,
    // 这个值小于当前的 watermask(10:58), 所以保持不变.(因为 watermask 只能增加不能减少)
  }
}
