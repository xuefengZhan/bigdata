package StructedStreaming._02_Source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * yatolovefantasy
 * 2021-10-14-19:00
 */
object kafka_source {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("kafkaSource").getOrCreate()

    import spark.implicits._
    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
       .option("subscribe", "stream")
      .load()
      .selectExpr("cast(value as string)")
      .as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()


    df.writeStream
      .format("console")
      .outputMode("update")
      .start
      .awaitTermination()
  }
}
