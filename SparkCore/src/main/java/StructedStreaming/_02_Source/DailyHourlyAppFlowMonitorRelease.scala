//package StructedStreaming._02_Source
//
//import com.alibaba.fastjson.JSON
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.functions.{date_format, to_timestamp}
//import org.apache.spark.sql.streaming.Trigger
//import org.apache.spark.sql.types.StringType
//import org.apache.spark.sql.{Dataset, Row, SparkSession}
//
//import java.sql.Timestamp
//import java.util.Date
//import java.util.concurrent.TimeUnit
//
//object DailyHourlyAppFlowMonitorRelease {
//  case class TraceEvent(is_test: Boolean,
//                        event: String,
//                        logtype:String,
//                        operatetype: String,
//                        var userid: String,
//                        __device_id: String,
//                        var time: Timestamp,
//                        app_id: String,
//                        dataid: String,
//                        datatype:String,
//                        plateviewid: String,
//                        pageduration: Long,
//                        pageid: String,
//                        page_data_id:String,
//                        page_data_type:String,
//                        __app_version: String,
//                        var sku_id:String
//                       )
//
//
//  def main(args: Array[String]): Unit = {
//    //屏蔽不必要的日志显示在终端上
//    Logger.getRootLogger.setLevel(Level.ERROR)
//
//    val conf = new SparkConf()
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.sql.streaming.checkpointLocation", "/tmp/spark/HourlyAppFlowMonitor")
//
//    val builder: SparkSession.Builder = SparkSession.builder().appName("HourlyAppFlowMonitor").config(conf).master("local[*]")
//    val spark: SparkSession = builder.getOrCreate()
//
//    import spark.implicits._
//
//    val batchTime = conf.getInt("spark.batch_time",60)//微批次批次时间（秒）
//
//    //从kafka读取埋点数据 只要value
//    val traceEvent: Dataset[TraceEvent] = spark.readStream.format("kafka")
//      .option("kafka.bootstrap.servers", "172.16.4.86:9092")
//      .option("subscribe","yjp_trace_v4_stream")
//      .option("startingOffsets", "latest")
//      .option("failOnDataLoss", false)
//      //.option("kafka.security.protocol", "SASL_PLAINTEXT")
//      .load()
//      .select($"value".cast(StringType)).map(_.getAs[String](0))
//      .map(x => {
//        val traceEvent = JSON.parseObject(x, classOf[TraceEvent])
//        if(traceEvent.userid == null || traceEvent.userid ==  ""){
//          traceEvent.userid = "0"
//        }
//        //1.将客户端时间修正
//        val now = new Date().getTime
//        if (traceEvent.time.getTime > now) traceEvent.time = new Timestamp(now)
//        //2.将sku_id统一
//        if(traceEvent.event == "exposure_product" || traceEvent.event == "view_operate"){
//          traceEvent.sku_id = traceEvent.dataid
//        }else if(traceEvent.event == "page_view"){
//          traceEvent.sku_id = traceEvent.page_data_id
//        }
//        traceEvent
//      })
//      .filter(_.is_test == false) //去除测试数据
//      .filter(x => { //保留6个事件
//        x.event match {
//          case "view_operate" => true
//          case "exposure_product" => true
//          case "product_search_result" => true
//          case "page_view" => true
//          case "ordersubmit" => true
//          case "ordersettle" => true
//          case _ => false
//        }
//      })
//      .filter(x => {
//        x.app_id.toUpperCase() match {
//          case "YJP-TR-AD001" => true
//          case "YJP-TR-WS001" => true
//          case "YJP-TR-IOS" => true
//          case _ => false
//        }
//      })
//
//
//
//
//    val dailyTraceEvent: Dataset[Row] = traceEvent.withColumn("event_day", to_timestamp(date_format($"time", "yyyy-MM-dd")))
//      .withColumn("date_key", date_format($"time", "yyyyMMdd"))
//      .withWatermark("event_day", "1 days 1 seconds")
//
//
//    //全量添加了天为单位的waterMark的埋点事件
//    dailyTraceEvent.createOrReplaceTempView("trace_event_daily")
//
//
//
//
//    //todo 3.天 User
//    //操作类型（operateType）	0	普通点击 1	曝光 2	加购  3	收藏
//    spark.sql(
//      """
//        |select
//        |      cast(t.date_key as int) date_key
//        |     ,CASE
//        |           when upper(t.app_id) = 'YJP-TR-AD001' then '安卓商城'
//        |           when upper(t.app_id) = 'YJP-TR-WS001' then '小程序'
//        |           when upper(t.app_id) = 'YJP-TR-IOS'   then '苹果商城'
//        |           else null end  as __app_id
//        |     ,t.__app_version
//        |     ,t.userid
//        |     ,max(time) as time
//        |     ,'stream_dm' as __database_name
//        |     ,'stream_trace_user_app_daily' as __table_name
//        |     ,sum(case when t.event = 'page_view' and t.logType='1'  then 1 else 0 end) as user_pv                                                              --用户访问页面数
//        |     ,approx_count_distinct(__device_id) as user_device_cnt                                                                                             --用户的设备数
//        |     ,sum(case when t.event = 'view_operate' and t.operatetype in ('0','2','3') or t.event = 'exposure_product' then 1 else 0 end) as user_active_cnt   --用户活跃次数
//        |     ,sum(case when t.event = 'page_view' and t.logtype = '2' then coalesce(t.pageduration,0) else 0 end)  as user_total_pageduration                   --用户的app使用时长
//        |     ,sum(case when  t.event = 'view_operate' and t.operatetype not in ('0','2','3') then 1 else 0 end) as user_jumploss_cnt                            --用户跳失操作次数
//        |     ,sum(case when  t.event = 'ordersubmit' then 1 else 0 end) as user_ordersubmit_cnt                                                                 --用户订单提交次数
//        |     ,sum(case when  t.event = 'ordersettle' then 1 else 0 end) as user_ordersettle_cnt                                                                 --用户订单结算次数
//        |     ,sum(case when  t.event = 'exposure_product' then 1 else 0 end) as  user_exposure_cnt                                                              --用户曝光次数
//        |     ,sum(case when  t.event = 'exposure_product' and t.pageid='ProductList' then 1 else 0 end) as  user_exposure_by_search_cnt                         --用户搜索曝光次数
//        |     ,sum(case when  t.event = 'exposure_product' and t.pageid = 'RecommendIndex' and t.plateviewid in('为你推荐','猜你喜欢') then 1 else 0 end) as  user_exposure_by_recomment_cnt  --用户推荐板块曝光次数
//        |     ,sum(case when  t.event = 'view_operate' and t.datatype = '1' and t.operatetype in ('3','2','0') then 1 else 0 end) as user_view_cnt               --用户访问sku的次数(点击、加购、收藏)
//        |     ,sum(case when  t.event = 'view_operate' and t.datatype = '1' and t.operatetype = '3' then 1 else 0 end) as  user_collect_cnt                      --用户收藏商品次数
//        |     ,sum(case when  t.event = 'view_operate' and t.datatype = '1' and t.operatetype = '2' then 1 else 0 end) as  user_addcar_cnt                       --用户加购商品次数
//        |     ,sum(case when  t.event = 'page_view' and t.pageid  = 'Product' then 1 else 0 end)  as  user_view_product_cnt                                      --用户详情商品页浏览数
//        |from
//        |     trace_event_daily t
//        |group by
//        |     t.event_day,t.date_key,t.__app_version,t.app_id,t.userid
//        |""".stripMargin)
//      .writeStream
//      .outputMode("update")
//      .format("console")
//      .queryName("DailyAppFlowMonitorUser")
//      .trigger(Trigger.ProcessingTime(batchTime, TimeUnit.SECONDS))
//      .start()
//
//
//    //todo 4.天 sku
//    spark.sql(
//      """
//        |select
//        |      cast(t.date_key as int) date_key
//        |      ,CASE
//        |           when upper(t.app_id) = 'YJP-TR-AD001' then '安卓商城'
//        |           when upper(t.app_id) = 'YJP-TR-WS001' then '小程序'
//        |           when upper(t.app_id) = 'YJP-TR-IOS'   then '苹果商城'
//        |           else null end  as __app_id
//        |       ,t.__app_version
//        |       ,t.sku_id
//        |       ,max(time) as time
//        |       ,'stream_dm' as __database_name
//        |       ,'stream_trace_sku_app_daily' as __table_name
//        |       ,sum(case when  t.event = 'exposure_product' then 1 else 0 end) as  exposure_cnt                                                         --商品曝光次数
//        |       ,sum(case when  t.event = 'exposure_product' and t.pageid = 'ProductList' then 1 else 0 end) as  exposure_by_search_cnt                  --商品搜索曝光次数
//        |       ,sum(case when  t.event = 'exposure_product' and t.pageid = 'RecommendIndex' and t.plateviewid in('为你推荐','猜你喜欢') then 1 else 0 end) as  exposure_by_recommend_cnt   --商品推荐版块曝光次数
//        |       ,sum(case when  t.event = 'view_operate' and t.datatype = '1' and t.operatetype in ('3','2','0') then 1 else 0 end) as product_visit_cnt --商品访问次数(点击、加购、收藏)
//        |       ,sum(case when  t.event = 'view_operate' and t.datatype = '1' and t.operatetype = '3' then 1 else 0 end) as  product_collect_cnt         --商品收藏次数
//        |       ,sum(case when  t.event = 'view_operate' and t.datatype = '1' and t.operatetype = '2' then 1 else 0 end) as  product_addcar_cnt          --商品加购次数
//        |       ,sum(case when  t.event = 'page_view' and t.pageid = 'Product' and t.sku_id is not null then 1 else 0 end ) as product_view_cnt          --商品详情页浏览次数
//        |from
//        |       trace_event_daily t
//        |group by
//        |       t.event_day,t.date_key,t.__app_version,t.app_id,t.sku_id
//        |""".stripMargin)
//      .writeStream
//      .outputMode("update")
//      .format("console")
//      .queryName("DailyAppFlowMonitorSku")
//      .trigger(Trigger.ProcessingTime(batchTime, TimeUnit.SECONDS))
//      .start()
//
//
//    //todo 3.访客数
//    //操作类型（operateType）	0	普通点击 1	曝光 2	加购  3	收藏
//    spark.sql(
//      """
//        |select
//        |      cast(t.date_key as int) date_key
//        |      ,CASE
//        |           when upper(t.app_id) = 'YJP-TR-AD001' then '安卓商城'
//        |           when upper(t.app_id) = 'YJP-TR-WS001' then '小程序'
//        |           when upper(t.app_id) = 'YJP-TR-IOS'   then '苹果商城'
//        |           else null end  as __app_id
//        |      ,t.__app_version
//        |     ,'stream_dm' as __database_name
//        |     ,'stream_trace_device_count_daily' as __table_name
//        |     ,approx_count_distinct(case when userid <> '0' then __device_id else null end ) as login_device_cnt
//        |     ,approx_count_distinct(__device_id) as device_cnt
//        |from
//        |     trace_event_daily t
//        |group by
//        |     t.event_day,t.date_key,t.__app_version,t.app_id
//        |""".stripMargin)
//      .writeStream
//      .outputMode("update")
//      .format("console")
//      .queryName("DailyAppFlowMonitorDevice")
//      .trigger(Trigger.ProcessingTime(batchTime, TimeUnit.SECONDS))
//      .start()
//      .awaitTermination()
//
//  }
//}
