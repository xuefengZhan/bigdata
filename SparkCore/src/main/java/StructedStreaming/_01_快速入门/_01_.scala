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
      .option("host", "192.168.196.128")
      .option("port", 9999)
      .load()

    // Split the lines into words
    // as[String] 将DataFrame转换为 Dataset,一行代表一个对象
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))


    //todo 结果表
    // Generate running word count
    val wordCounts: DataFrame = words.groupBy("value").count()


    //start running the query
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    //todo 输出模式
    //complete  整个更新的结果表会被写入到外部存储
    //Append Mode 从上次触发结束开始算起, 仅仅把那些新追加到结果表中的行写到外部存储
    //该模式仅适用于不会更改结果表中行的那些查询. (如果有聚合操作, 则必须添加 wartemark, 否则不支持此种模式)
    //Update Mode 从上次触发结束开始算起, 仅仅在结果表中更新的行会写入到外部存储

    //Update Mode 与 Complete Mode 的不同在于 Update Mode 仅仅输出改变的那些行. 如果查询不包括聚合操作, 则等同于
    //Append Mode

    //complete =  update + append
    //没有聚合操作，则只有append

    query.awaitTermination()

    spark.stop()


  }
}
