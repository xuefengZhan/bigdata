package StructedStreaming._02_Source

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{date_format, to_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import java.sql.Timestamp
import java.util.Date
import java.util.concurrent.TimeUnit

object AppMonitor2 {
  case class TraceEvent(is_test: Boolean,
                        event: String,
                        logtype:String,
                        operatetype: String,
                        userid: Long,
                        device_id: String,
                        var time: Timestamp,
                        user_cityid: Long,
                        var city_region_id: Int,
                        app_id: String,
                        dataid: String,
                        keywords: String,
                        plateviewid: String,
                        pageduration: Long,
                        pageid: String,
                        session_event: String,
                        __app_version: String,
                        __lib: String
                       )


  def main(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getRootLogger.setLevel(Level.ERROR)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.streaming.checkpointLocation", "E:/tmp/spark/MonitorApp")

    val builder: SparkSession.Builder = SparkSession.builder().master("local[*]").appName("AppFlowMonitor").config(conf)
    val spark: SparkSession = builder.getOrCreate()

    import spark.implicits._

    val batchTime = conf.getInt("spark.batch_time",60)//微批次批次时间（秒）

    //从kafka读取埋点数据 只要value
    val traceEvent: Dataset[Row] = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "172.16.4.86:9092")
      .option("subscribe", "yjp_trace_v4_stream")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .option("kafka.topic.id", "zxf_test2")
      .load()
      .select($"value".cast(StringType)).map(_.getAs[String](0))
      .map(x => { //将客户端时间修正
        val traceEvent = JSON.parseObject(x, classOf[TraceEvent])
        val now = new Date().getTime
        if (traceEvent.time.getTime > now) traceEvent.time = new Timestamp(now)
        traceEvent
      })
      .filter(_.is_test == false) //祛除测试数据
      .filter(x => { //保留四个事件
        x.event match {
          case "view_operate" => true
          case "exposure_product" => true
          case "product_search_result" => true
          case "page_view" => true
          case _ => false
        }
      })
      .filter(x => {
        x.app_id.toUpperCase() match {
          case "YJP-TR-AD001" => true
          case "YJP-TR-WS001" => true
          case "YJP-TR-IOS" => true
          case _ => false
        }
      })
      .withColumn("event_day", to_timestamp(date_format($"time", "yyyy-MM-dd"))) //事件时间
      .withColumn("date_key", date_format($"time", "yyyyMMdd"))
      .withWatermark("event_day", "1 days 1 seconds")

    // 全量的埋点事件
    traceEvent.createOrReplaceTempView("trace_event_all")

   // traceEvent.writeStream.format("console").option("truncate", "false").outputMode("update").start().awaitTermination()

    //操作类型（operateType）	0	普通点击 1	曝光 2	加购  3	收藏
    spark.sql(
      """
        |select
        |     t.date_key
        |     ,t.__app_version
        |     ,t.__lib
        |     ,count(case when event = 'page_view' and logtype = '1' then 1 end) as pv  --只记录进入页面的
        |     ,approx_count_distinct(t.userid) as uv   --uv
        |     ,approx_count_distinct(case when t.event = 'view_operate' and t.operatetype in ('0','1') then t.userid end) as active_user_cnt  --3.活跃用户数
        |     ,sum(case when t.event = 'page_view' then t.pageduration end ) total_ppageduration       -- 总的app使用时长时长 event=pageview
        |     ,approx_count_distinct(case when  t.event = 'view_operate' and t.operatetype not in ('0','1','2','3') then t.userid end) as jumploss_user_cnt  --6.跳失用户，进入应用后没有做任何点击行为就退出的用户
        |     ,approx_count_distinct(case when  t.event = 'view_operate' and t.pageid = 'OrderComplete' then t.userid end) as orderComplete_user_cnt  --订单提交并购买成功的客户人数
        |     ,approx_count_distinct(case when  t.event = 'exposure_product' then t.userid end) as  exposure_user_cnt  --曝光的人数
        |     ,approx_count_distinct(case when  t.event = 'exposure_product' and t.pageid='ProductList' then t.userid end) as  exposure_by_search_user_cnt  --搜索曝光人数
        |     ,approx_count_distinct(case when  t.event = 'exposure_product' and t.plateviewid in ('productlist_recommend','recommend_product_combine_view','promotion_calendar_view') then t.userid end) as  exposure_by_recomment_user_cnt  --推荐板块曝光的人数
        |     ,approx_count_distinct(case when  t.event = 'view_operate' and t.operatetype in ('2','0') then t.userid end) as view_user_cnt --访问/浏览用户数
        |     ,approx_count_distinct(case when  t.event = 'view_operate' and t.operatetype = '3' then t.userid end) as  collect_user_cnt   --收藏用户数
        |     ,approx_count_distinct(case when  t.event = 'view_operate' and t.operatetype = '2' then t.userid end) as  shoppingcar_user_cnt   --加购用户数
        |     ,count(case when  t.event = 'exposure_product' then t.dataid end) as  exposure_product_cnt  --商品曝光次数
        |     ,count(case when  t.event = 'exposure_product' and t.pageid = 'ProductList' then t.dataid end) as  exposure_by_search_product_cnt   --商品搜索曝光次数
        |     ,count(case when  t.event = 'exposure_product' and t.plateviewid in ('productlist_recommend','recommend_product_combine_view','promotion_calendar_view') then t.dataid end) as  exposure_by_recommend_product_cnt   --推荐版块商品曝光次数
        |     ,count(case when  t.event = 'page_view' and t.pageid = 'Product' then t.dataid end) as  product_view_cnt   --商品页浏览次数
        |     ,count(case when  t.event = 'view_operate' and t.operatetype = '3' then t.dataid end) as  product_collect_cnt --商品收藏次数
        |     ,count(case when  t.event = 'view_operate' and t.operatetype = '2' then t.dataid end) as  product_shopingcar_cnt  --商品加购次数
        |     ,count(case when  t.event = 'exposurce_product' and t.pageid = 'OrderComplete' then t.dataid end) as product_ordercomplete_cnt  --商品下单次数
        |     ,approx_count_distinct(case when t.userid is null then t.device_id end) as visitor_cnt  --访客数
        |from
        |     trace_event_all t
        |group by
        |     t.event_day,t.date_key,t.__app_version,t.__lib
        |""".stripMargin)
      .writeStream
      .outputMode("update")
      .format("console")
      .queryName("AppFlowMonitor")
      .trigger(Trigger.ProcessingTime(batchTime, TimeUnit.SECONDS))
      .start()
      .awaitTermination()
  }
}
