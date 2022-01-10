package StructedStreaming._02_Source

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}



object file_source {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StructuredNetworkWordCount").master("local[*]").getOrCreate()

    import spark.implicits._

    val userSchema : StructType = new StructType()
      .add("name", StringType)
      .add("age", LongType)
      .add("sex", StringType)

    val src: DataFrame = spark.readStream
      .format("csv")
      .schema(userSchema)
      .load("data/user.csv")

    val result: DataFrame = src.groupBy("sex").sum("age")

    result.writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime(1000))
      .start()
      .awaitTermination()
  }
}
