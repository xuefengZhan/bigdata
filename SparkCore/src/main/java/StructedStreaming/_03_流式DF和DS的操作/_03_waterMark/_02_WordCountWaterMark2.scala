package StructedStreaming._03_流式DF和DS的操作._03_waterMark

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.sql.Timestamp

/**
 * todo. apend 输出模式下 waterMark的效果演示
 */
object _02_WordCountWaterMark2 {
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
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .option("truncate", "false")
      .start
    query.awaitTermination()


    //在 append 模式中, 仅输出新增的数据, 且输出后的数据无法变更.
    //1.输入数据:2019-08-14 10:55:00,dog   第一批数据  waterMark = 0

    //(1) 获取窗口
    //按照window($"timestamp", "10 minutes", "2 minutes")得到 5 个窗口
    //
    // (2)比较窗口结束时间和waterMark的关系
    // 当前批次中所有窗口的结束时间均大于 watermask.
    // 但是 Structured Streaming 无法确定后续批次的数据中是否会更新当前批次的内容.
    // 因此, 基于 Append 模式的特点, 这时并不会输出任何数据(因为输出后数据就无法更改了),
    // 直到某个窗口的结束时间小于 watermark,
    // 即可以确定后续数据不会再变更该窗口的聚合结果时才会将其输出, 并移除内存中对应窗口的聚合状态.
    // (3) 更新waterMark
    // watermark=10:53

    //+------+----+-----+
    //|window|word|count|
    //+------+----+-----+
    //+------+----+-----+

    //2. 输入数据:2019-08-14 11:00:00,dog    watermark=10:53
    //(1)计算得到 5 个窗口
    //
    //(2)比较窗口结束时间和waterMark的关系
    // 所有的窗口的结束时间均大于 watermark, 仍然不会输出.
    //+------+----+-----+
    //|window|word|count|
    //+------+----+-----+
    //+------+----+-----+

    // (3) 更新waterMark
    // watermark = 11:00 - 2min = 10:58

    //3. 输入数据:2019-08-14 10:55:00,dog
    //
    // (1) 计算出每个窗口
    //
    // (2)比较窗口结束时间和waterMark的关系
    // 当前内存中有两个窗口的结束时间已经低于 10: 58.
    //|[2019-08-14 10:48:00, 2019-08-14 10:58:00]|dog |1    |
    //|[2019-08-14 10:46:00, 2019-08-14 10:56:00]|dog |1    |
    // 则意味着这两个窗口的数据不会再发生变化, 此时输出这个两个窗口的聚合结果, 并在内存中清除这两个窗口的状态.
    //所以这次输出结果:
    //+------------------------------------------+----+-----+
    //|window                                    |word|count|
    //+------------------------------------------+----+-----+
    //|[2019-08-14 10:46:00, 2019-08-14 10:56:00]|dog |1    |
    //|[2019-08-14 10:48:00, 2019-08-14 10:58:00]|dog |1    |
    //+------------------------------------------+----+-----+
    // (3) 更新waterMark
    // 第三个批次的数据处理完成后, 立即计算: watermark= 10:55 - 2min = 10:53,
    // 这个值小于当前的 watermask(10:58), 所以保持不变.
    // (因为 watermask 只能增加不能减少)

  }
}
