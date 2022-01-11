//package SparkStreaming
//
//import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.json4s.jackson.Json
//
//object DirectAPI {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")
//    val ssc = new StreamingContext(conf, Seconds(3))
//
//
//    //{"__model":"JER-AN20","logintype":5,"__os":"Android","__app_version":"3.55.0","refpageid":"ZoneIndex","__screen_width":1080,"__os_version":"10","dataposition":"52","version_name":"3.55.0","session_event":"Product_Search_Result_e04d40ace9e349f3ae67a99b5cd3d7c7","userid":"302785","distinct_id":"WL22ydev-G5Ri-5SNc-ADoe-TrCqLFl31Xid00","__manufacturer":"HUAWEI","datatype":"1","promotionname":"洁能限时折扣","specname":"1卷","__is_first_day":false,"operevent":"ProductExposure","__lib_method":"code","dataname":"洁能点断式保鲜袋200只装25*38cm","_flush_time":"2021-10-11 15:25:49","session_pagecreate":"4e0094d519a14263bef1d8c321cdc385","__wifi":true,"id":"7581277e-2a64-11ec-9dd6-deb48949dbc2","__network_type":"WIFI","promotiontype":"4","__lib":"Android","__carrier":"中国联通","__lib_version":"3.2.7_3","patch_version":"3.55.0-0","time":"2021-10-11 15:25:47","__screen_height":2340,"session_duration":"031d4ff6d00145bf8907d2002c88b4aa","pageid":"ProductList","is_test":false,"session_pageshow":"bf99fd6a644946ac86b877eb6ccbce3d","__device_id":"WL22ydev-G5Ri-5SNc-ADoe-TrCqLFl31Xid00","event":"exposure_product","type":"track","session_app":"8d379ea7c4c04901b98f82eb1deedde6","dataid":"40200151303326","_tbl_name":"ods_trd_shopmall_exposure_product","plateviewid":"productlist","user_cityid":428,"_track_id":"-1175161787","app_id":"YJP-TR-AD001"}
//    //todo 定义kafka消费者参数
//    val kafkaPara: Map[String, Object] = Map[String, Object](
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "datanode1.release.yjp.com:9092,datanode2.release.yjp.com:9092,datanode3.release.yjp.com:9092,datanode4.release.yjp.com:9092,datanode15.release.yjp.com:9092",
//      ConsumerConfig.GROUP_ID_CONFIG -> "zxf",
//      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
//      "security.protocol"-> "SASL_PLAINTEXT",
//      "sasl.kerberos.service.name"->"kafka"
//    )
//
//
//  //todo 消费kafka 创建DStream  泛型是KV类型
//  val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
//    LocationStrategies.PreferConsistent,
//    ConsumerStrategies.Subscribe[String, String](Set("yjp_trace_v4_pro"), kafkaPara))
//
//
//    //5.将每条消息的KV取出
//    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())
//
//    valueDStream.print()
//    //7.开启任务
//    ssc.start()
//    ssc.awaitTermination()
//
//
//
//    //6.计算WordCount
////    valueDStream.flatMap(_.split(" "))
////      .map((_, 1))
////      .reduceByKey(_ + _)
////      .print()
////
////    //7.开启任务
////    ssc.start()
////    ssc.awaitTermination()
//
//
//
//  }
//}
