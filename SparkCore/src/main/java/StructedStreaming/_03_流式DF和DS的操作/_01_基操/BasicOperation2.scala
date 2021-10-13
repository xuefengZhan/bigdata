package StructedStreaming._03_流式DF和DS的操作._01_基操

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
 * 强类型
 */

case class People(name: String, age: Long, sex: String)
object BasicOperation2 {
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

    // todo 强类型 给DF增加类型
    // 转换DS要 导包
    import spark.implicits._
    val peopleDS: Dataset[People] = peopleDF.as[People] // 转成 ds

    //val df: DataFrame = peopleDF.select("name","age", "sex").where("age > 20")
    val df: Dataset[String] = peopleDS.filter(_.age > 20).map(_.name)

    df.writeStream
      .outputMode("append")
      .format("console")
      .start
      .awaitTermination()


  }
}
