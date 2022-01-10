package StructedStreaming._01_快速入门

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object _01_ {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .config(conf)
      .getOrCreate()

    import spark.implicits._


    //todo 1.输入表
    // an unbounded table
    // one column of strings named “value”,
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("port", 9999)
      .load()

    //spark.readStream返回的是一个 DataStreamReader
    //通过load方法，读取数据获取输入表

    //todo 2.业务逻辑
    // Split the lines into words
    // as[String] 将DataFrame转换为 Dataset,一行代表一个对象
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))



    //todo 3.结果表
    // Generate running word count
    // 只有一列，就用value做列名
    val wordCounts: DataFrame = words.groupBy("value").count()


    //start running the query
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    //todo 输出模式
    // 1. complete  整个更新的结果表会被写入到外部存储  第一次一条 第二次两条 第三次三条  一共有多少输出多少
    // 2. Append Mode 用于窗口，输出用内容已经固定了的窗口
    // 该模式仅适用于不会更改结果表中行的那些查询. (如果有聚合操作, 则必须添加 wartemark, 否则不支持此种模式)
    // 3.Update Mode 从上次触发结束开始算起, 仅仅在结果表中更新的行会写入到外部存储

    //Update Mode 与 Complete Mode 的不同在于 Update Mode 仅仅输出改变的那些行. 如果查询不包括聚合操作, 则等同于
    //Append Mode

    //complete =  update + append
    //没有聚合操作，则只有append

    query.awaitTermination()

    spark.stop()


  }
}
