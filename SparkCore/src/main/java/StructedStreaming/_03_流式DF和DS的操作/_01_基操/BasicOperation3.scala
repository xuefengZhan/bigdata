package StructedStreaming._03_流式DF和DS的操作._01_基操

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 弱类型
 */
object BasicOperation3 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("BasicOperation")
      .getOrCreate()

    //弱类型，指定字段
    val peopleSchema: StructType = new StructType()
      .add("name", StringType)
      .add("age", LongType)
      .add("sex", StringType)

    val peopleDF: DataFrame = spark.readStream
      .schema(peopleSchema)
      .json("data")
    // todo 弱类型 sql方式
    //val df: DataFrame = peopleDF.select("name","age", "sex").where("age > 20")
    // todo 1.从DF创建临时表
    peopleDF.createOrReplaceTempView("people")

    val df: DataFrame = spark.sql("select * from people where age > 20 ")


    df.writeStream
      .outputMode("append")
      .format("console")
      .start
      .awaitTermination()

  }
}
