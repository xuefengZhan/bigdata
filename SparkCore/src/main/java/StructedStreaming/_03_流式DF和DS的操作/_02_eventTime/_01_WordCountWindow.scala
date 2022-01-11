package StructedStreaming._03_流式DF和DS的操作._02_eventTime

import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object _01_WordCountWindow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("wc").getOrCreate()

    import spark.implicits._


    //todo 1.数据源 readStream创建流式DataFrame
    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "192.168.196.128")
      .option("port", 9999)
      .option("includeTimestamp", true) // 给产生的数据自动添加时间戳  时间戳和数据构成tuple =》 (一行数据，时间戳)
      .load

    //todo 2.业务逻辑
    val lines1: Dataset[(String, Timestamp)] = lines.as[(String, Timestamp)]
    //切割成单词，带时间戳
    val words = lines1.flatMap(line => {
      line._1.split(" ").map((_, line._2))
    }).toDF("word", "timestamp")

    import org.apache.spark.sql.functions._
    //统计每个窗口的每个word的个数
    //window是一个函数，返回值是要给新的列
    val wordCounts: Dataset[Row] = words.groupBy(
      //调用 window 函数, 返回的是一个 Column
      //参数 1: df 中表示时间戳的列 参数 2: 窗口长度 参数 3: 滑动步长
      window(to_timestamp($"timestamp"), "10 seconds", "5 seconds"),
      $"word"
    ).count().orderBy($"window") // 计数, 并按照窗口排序


    //todo 3.输出
    val query: StreamingQuery = wordCounts.writeStream.outputMode("complete")
      .format("console")
      .option("truncate", "false") // 不截断.为了在控制台能看到完整信息, 最好设置为 false
      .start

    query.awaitTermination()
  }
}
