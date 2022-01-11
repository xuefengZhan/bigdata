package StructedStreaming._02_Source


import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}


object KafkaSourceDemo {
    def main(args: Array[String]): Unit = {
      val spark: SparkSession = SparkSession
        .builder()
        .master("local[*]")
        .appName("KafkaSourceDemo")
        .getOrCreate()

      // 得到的 df 的 schema 是固定的: key,value,topic,partition,offset,timestamp,timestampType
      val df: DataFrame = spark.readStream
        .format("kafka") // 设置 kafka 数据源
        .option("kafka.bootstrap.servers", "172.16.4.86:9092")
        .option("subscribe", "stream")
        .option("startingOffsets", "latest")
       // .option("kafka.group.id","stream_zxf")
        .load()



      df.writeStream
        .format("console")
        .trigger(Trigger.ProcessingTime(0))
        .start()
        .awaitTermination()
    }
}


