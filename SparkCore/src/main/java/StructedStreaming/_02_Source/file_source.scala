package StructedStreaming._02_Source

import org.apache.spark.sql.SparkSession

object _02_source {
  val spark = SparkSession.builder().appName("StructuredNetworkWordCount").getOrCreate()

  //  new StructType().add
  //  spark.readStream.option("sep",";").schema()
}
