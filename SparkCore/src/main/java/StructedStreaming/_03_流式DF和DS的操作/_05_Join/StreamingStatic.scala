package StructedStreaming._03_流式DF和DS的操作._05_Join

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 *  streaming df/ds 和 静态的ds/df   join
 */
object StreamingStatic {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("StreamingStatic")
      .getOrCreate()
    import spark.implicits._
    // 1. 静态 df
    val arr = Array(("lisi", "male"), ("zhiling", "female"), ("zs", "male"));
    var staticDF: DataFrame = spark.sparkContext.parallelize(arr).toDF("name", "sex")

    // 2. streaming df
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "fantasy")
      .option("port", 9999)
      .load()

    val streamDF: DataFrame = lines.as[String].map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toInt)
    }).toDF("name", "age")

    // 3. inner join
    //val joinResult: DataFrame = streamDF.join(staticDF, "name")

    //lisi,20
    //zhiling,40
    //ww,30

    //+-------+---+------+
    //|   name|age|   sex|
    //+-------+---+------+
    //|zhiling| 40|female|
    //|   lisi| 20|  male|
    //+-------+---+------+
    // a join b  a 的字段在前面

    // 4.left out join
    val joinResult: DataFrame = streamDF.join(staticDF, Seq("name"), "left")
    // +-------+---+------+
    //|   name|age|   sex|
    //+-------+---+------+
    //|zhiling| 40|female|
    //|     ww| 30|  null|
    //|   lisi| 20|  male|
    //+-------+---+------+


    joinResult.writeStream
      .outputMode("append")
      .format("console")
      .start
      .awaitTermination()


  }
}
