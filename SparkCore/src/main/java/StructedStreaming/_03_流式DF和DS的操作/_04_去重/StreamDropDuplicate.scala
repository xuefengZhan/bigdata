package StructedStreaming._03_流式DF和DS的操作._04_去重

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.sql.Timestamp

object StreamDropDuplicate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("wc").getOrCreate()

    import spark.implicits._

    //todo 1.数据源
    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "192.168.196.128")
      .option("port", 9999)
      .load



    val ds: Dataset[(String, Timestamp, String)] = lines.as[String].map(line => {
      val split = line.split(",")
      (split(0), Timestamp.valueOf(split(1)), split(2))
    })

    val words: DataFrame = ds.toDF("uid", "ts", "word")

    val wordCounts: Dataset[Row] = words
      .withWatermark("ts", "2 minutes")
      .dropDuplicates("uid")  // 去重重复数据 uid 相同就是重复.  可以传递多个列

    wordCounts.writeStream
      .outputMode("append")
      .format("console")
      .start
      .awaitTermination()

    //测试
    // (1) wm = 0
    //1.第一批: 1,2019-09-14 11:50:00,dog
    // (2) timestamp > wm 并且 [wm,~) 无重复  所以输出
    //+---+-------------------+----+
    //|uid|                 ts|word|
    //+---+-------------------+----+
    //|  1|2019-09-14 11:50:00| dog|
    //+---+-------------------+----+
    // (3) 更新wm=48

    // (1)  wm=48
    //2.第 2 批: 2,2019-09-14 11:51:00,dog
    // (2) timestamp > wm 并且 [wm,~) 无重复  所以输出
    //+---+-------------------+----+
    //|uid|                 ts|word|
    //+---+-------------------+----+
    //|  2|2019-09-14 11:51:00| dog|
    //+---+-------------------+----+
    // (3)  更新wm=49

    //(1) wm=49
    //3.第 3 批: 1,2019-09-14 11:50:00,dog
    //(2) timestamp > wm  没过期 但是 [wm,~) id 重复  所以无输出
    //(3) wm=49

    //(1) wm=49
    //4.第 4 批: 3,2019-09-14 11:53:00,dog
    //(2) timestamp > wm 并且 [wm,~) id 重复无输出
    //+---+-------------------+----+
    //|uid|                 ts|word|
    //+---+-------------------+----+
    //|  3|2019-09-14 11:53:00| dog|
    //+---+-------------------+----+
    //(3) watermask=11:51

    // (1) wm=51
    //5.第 5 批: 1,2019-09-14 11:50:00,dog
    // (2)timestamp < wm 数据过期    所以无输出

    //6. 第 6 批 4,2019-09-14 11:45:00,dog 数据过时, 所以无输出


    //7. 第7批   1,2019-09-14 11:52:00,dog
    // 测试wm>51的还输出不输出

    //append模式下：
    // 只输出  timestamp > wm 并且  在[wm,~)范围内没有重复过的数据
  }
}
