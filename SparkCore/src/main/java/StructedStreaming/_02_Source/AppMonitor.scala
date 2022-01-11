package StructedStreaming._02_Source

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{date_format, to_timestamp}
import org.apache.spark.sql.types.StringType
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.sql.Timestamp
import java.util.Date

object AppMonitor {
  case class TraceEvent(is_test: Boolean,
                        event: String,
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
      .set("spark.sql.streaming.checkpointLocation", "E:/tmp/spark/TramcBillingApp")

    val builder: SparkSession.Builder = SparkSession.builder().master("local[*]").appName("AppFlowMonitor").config(conf)
    val spark: SparkSession = builder.getOrCreate()

    import spark.implicits._
    //val batchTime = conf.getInt("spark.batch_time",60)//微批次批次时间（秒）

    //从kafka读取埋点数据 只要value
    val traceEvent: Dataset[Row] = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "172.16.4.86:9092")
      .option("subscribe", "yjp_trace_v4_stream")
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", false)
      .option("kafka.topic.id", "zxf_test")
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


    //traceEvent.writeStream.format("console").outputMode("update").start().awaitTermination()

    // 1. dataFrame1 : 全量的埋点事件
    traceEvent.createOrReplaceTempView("trace_event_all")
    // 2. dataFrame2 : 非访客的事件
   traceEvent.filter("userid is not null").createOrReplaceTempView("trace_event_no_visitor")


        //todo 1. pv （用户点击总次数_不含访客）每一次页面被点击，同一页面被点击多次，计多次，count(1)
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(1) as pv
            |from (
            |select
            |      distinct event_day,date_key,userid,__app_version,__lib
            |      from
            |           trace_event_no_visitor
            |      where
            |            event = 'page_view'
            |     ) t
            |group by
            |     t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)
          .writeStream
          .outputMode("update")
          .format("console")
          .queryName("pv")
          .trigger(Trigger.ProcessingTime(0))
          .start()


       //todo 2.uv  （用户数_不含访客） 只含登录用户不含访客
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(1) as uv
            |from
            |    (select distinct event_day,date_key,userid,__app_version,__lib
            |     from trace_event_no_visitor
            |    ) t
            |group by
            |    t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)
          .writeStream
          .outputMode("update")
          .format("console")
          .queryName("uv")
          .trigger(Trigger.ProcessingTime(0))
          .start()



        //todo 3. 活跃用户数  产生了商品曝光或有点击行为的去重用户数， count(distinct userid)
        //操作类型（operateType）	0	普通点击 1	曝光 2	加购  3	收藏
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(1) as active_user_cnt
            |from
            |      (select distinct event_day,date_key,userid,__app_version,__lib
            |       from trace_event_no_visitor
            |       where event = 'view_operate' and   operatetype in ('0','1')
            |      ) t
            |group by
            |    t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)
            .writeStream
            .outputMode("update")
            .format("console")
            .queryName("active_user_cnt")
            .trigger(Trigger.ProcessingTime(0))
            .start()
            .awaitTermination()


        //todo 4.访问用户数  没有登录用户id的用户记录device_id，统计uerid 为null,count(distinct device_id)的用户数
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(case when userid is null then 1 end) as visitor_cnt
            |from
            |     (select distinct event_day,date_key,device_id,userid,__app_version,__lib
            |      from trace_event_all
            |      where event = 'view_operate') t
            |group by
            |      t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)
                .writeStream
                .outputMode("update")
                .format("console")
                .queryName("active_user_cnt")
                .trigger(Trigger.ProcessingTime(0))
                .start()
                .awaitTermination()

    //    //todo 5. 人均访问次数   PV/UV

    //    //todo 6.app人均使用时长   全部的pageduration/uv
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     sum(pageduration) total_ppageduration          --ms
            |from
            |     trace_event_no_visitor t
            |group by
            |     t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)
          .writeStream
          .outputMode("complete")
          .format("console")
          .queryName("active_user_cnt")
          .trigger(Trigger.ProcessingTime(0))
          .start()
          .awaitTermination()

        //todo 7.跳失率 进入应用后没有做任何点击行为就退出的用户/全站UV
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(1) as no_action_user_cnt
            |from
            |     (select distinct event_day,date_key,userid,__app_version,__lib
            |      from trace_event_no_visitor
            |      where event = 'view_operate'
            |      and   operatetype not in ('0','1','2','3')
            |     )  t
            |group by
            |     t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)
          .writeStream
          .outputMode("update")
          .format("console")
          .queryName("active_user_cnt")
          .trigger(Trigger.ProcessingTime(0))
          .start()


        //todo 8.下单转化率 （订单提交并购买成功的客户人数÷全站UV）×100%
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(1) as orderComplete_user_cnt
            |from
            |     (select distinct event_day,date_key,userid,__app_version,__lib
            |      from trace_event_no_visitor
            |      where event = 'view_operate'
            |      and   pageid = 'OrderComplete'
            |     )  t
            |group by
            |     t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)
          .writeStream
          .outputMode("update")
          .format("console")
          .queryName("active_user_cnt")
          .trigger(Trigger.ProcessingTime(0))
          .start()

        //todo 2.1.登录用户数

        //todo 2.2.统计商品/活动/Banner/推荐弹框/店铺向用户曝光的人数,count(distinct userid)
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(1) as exposure_user_cnt,   --曝光人数
            |from
            |     (select distinct event_day,date_key,userid,__app_version,__lib
            |      from trace_event_no_visitor
            |      where event = 'exposure_product'
            |     )  t
            |group by
            |     t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)




    //    //todo 2.3.搜索出来的商品向用户曝光的人数，count(distinct userid)，where pageid='ProductList'
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(1) as exposure_by_search_user_cnt
            |from
            |     (select distinct event_day,date_key,userid,__app_version,__lib
            |      from trace_event_no_visitor
            |      where event = 'exposure_product' and pageid='ProductList'
            |      )  t
            |group by
            |     t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)


        //todo 2.4 推荐板块的商品向用户曝光的人数，count(distinct userid)，where待确认，，需要确认哪些页面的商品列表曝光属于推荐商品曝光
        //productlist_recommend 推荐产品列表  recommend_product_combine_view  推荐首页组合板块
        // promotion_calendar_view 推荐首页促销日历板块
        spark.sql(
          """
            |select
            |     t.date_key,
            |     t.__app_version,
            |     t.__lib,
            |     count(1) as exposure_by_recomment_user_cnt
            |from
            |     (select distinct event_day,date_key,userid,__app_version,__lib
            |      from trace_event_no_visitor
            |      where event = 'exposure_product'
            |      and plateviewid in ('productlist_recommend','recommend_product_combine_view','promotion_calendar_view')
            |      )  t
            |group by
            |     t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)


        //todo 2.5. 访问/浏览用户数    有点击或者加购行为的去重用户数
          spark.sql(
            """
              |select
              |     t.date_key,
              |     t.__app_version,
              |     t.__lib,
              |     count(1) as view_user_cnt
              |from
              |     (select distinct event_day,date_key,userid,__app_version,__lib
              |      from trace_event_no_visitor
              |      where  event = 'view_operate'
              |      and    operatetype in ('2','0')
              |      )  t
              |group by
              |     t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)

          //todo 2.6  收藏用户数   统计有点击收藏行为的去重用户总数，取消情况不考虑， count(distinct userid)
          spark.sql(
            """
              |select
              |     t.date_key,
              |     t.__app_version,
              |     t.__lib,
              |     count(1) as collect_user_cnt
              |from
              |     (select distinct event_day,date_key,userid,__app_version,__lib
              |      from trace_event_no_visitor
              |      where  event = 'view_operate'
              |      and    operatetype = '3'
              |      )  t
              |group by
              |     t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)



        //todo 2.7 加购用户数  统计有点击加购行为的去重用户总数， count(distinct userid)
          spark.sql(
            """
              |select
              |     t.date_key,
              |     t.__app_version,
              |     t.__lib,
              |     count(1) as shoppingcar_user_cnt
              |from
              |     (select distinct event_day,date_key,userid,__app_version,__lib
              |      from trace_event_no_visitor
              |      where  event = 'view_operate'
              |      and    operatetype = '2'
              |      )  t
              |group by
              |     t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)



//    todo 2.9 用户数  统计有点击加购行为的去重用户总数， count(distinct userid)
    spark.sql(
      """
        |select
        |     t.date_key,
        |     t.__app_version,
        |     t.__lib,
        |     count(case when operatetype = '2' then 1 end) as shoppingcar_user_cnt,  --加购用户数
        |     count(case when operatetype = '3' then 1 end) as collect_user_cnt,      --收藏用户数
        |     count(case when operatetype in ('2','0') then 1 end) as  view_user_cnt, --访问/浏览用户数
        |from
        |     (select distinct event_day,date_key,userid,operatetype,__app_version,__lib
        |      from trace_event_no_visitor
        |      where  event = 'view_operate'
        |      )  t
        |group by
        |     t.event_day,t.date_key,t.__app_version,t.__lib
        |""".stripMargin)


    //    //todo 2.8 下单用户数  提交订单并购买成功的去重用户数， count(distinct userid)
    //    //8.订单提交并购买完成的用户数  订单完成页面的数量
          // same with 1.8



        //todo 3.1.商品曝光次数 包含所有类型的商品曝光，曝光一次记一次，count（1）
        spark.sql(
          """
            |select
            |      t.date_key,
            |      t.__app_version,
            |      t.__lib,
            |      count(1) as exposure_product_cnt
            |from
            |      trace_event_no_visitor t
            |where
            |      event = 'exposure_product'
            |group by
            |       t.event_day,t.date_key,t.__app_version,t.__lib
            |""".stripMargin)

    //    //todo 3.2.搜索商品曝光次数

          spark.sql(
            """
              |select
              |      t.date_key,
              |      t.__app_version,
              |      t.__lib,
              |      count(1) as exposure_by_search_product_cnt
              |from
              |      trace_event_no_visitor t
              |where
              |      event = 'exposure_product'
              |and   pageid = 'ProductList'
              |group by
              |       t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)


    //    //todo 3.3 推荐商品曝光次数
          spark.sql(
            """
              |select
              |      t.date_key,
              |      t.__app_version,
              |      t.__lib,
              |      count(1) as exposure_by_recommend_product_cnt
              |from
              |      trace_event_no_visitor t
              |where
              |      event = 'exposure_product'
              |and   plateviewid in ('productlist_recommend','recommend_product_combine_view','promotion_calendar_view')
              |group by
              |       t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)


        //todo 3.4.商品页浏览次数   点击查看商品详情页的次数，基于页面事件计算，count(1)，where pageid='Product'
          spark.sql(
            """
              |select
              |      t.date_key,
              |      t.__app_version,
              |      t.__lib,
              |      count(1) as product_view_cnt
              |from
              |      trace_event_no_visitor t
              |where
              |      event = 'page_view'
              |and   pageid = 'Product'
              |group by
              |       t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)


          //todo 3.5 商品收藏次数   商品被加关注一次计入一次收藏，取消情况不考虑， count(1)
          spark.sql(
            """
              |select
              |      t.date_key,
              |      t.__app_version,
              |      t.__lib,
              |      count(1) as product_collect_cnt
              |from
              |      trace_event_no_visitor t
              |where
              |      event = 'view_operate'
              |and   operatetype = '3'
              |group by
              |       t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)



        //todo 3.6 商品加购次数
          spark.sql(
            """
              |select
              |      t.date_key,
              |      t.__app_version,
              |      t.__lib,
              |      count(1) as product_shopingcar_cnt
              |from
              |      trace_event_no_visitor t
              |where
              |      event = 'view_operate'
              |and   operatetype = '2'
              |group by
              |       t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)

    //todo 3.6 商品收藏和加购次数
              spark.sql(
                """
                  |select
                  |      t.date_key,
                  |      t.__app_version,
                  |      t.__lib,
                  |      count(case when operatetype = '2' then 1 end) as product_shopingcar_cnt --加购次数
                  |      count(case when operatetype = '3' then 1 end) as product_collect_cnt    --收藏次数
                  |from
                  |      trace_event_no_visitor t
                  |where
                  |      event = 'view_operate'
                  |and   operatetype = '2'
                  |group by
                  |       t.event_day,t.date_key,t.__app_version,t.__lib
                  |""".stripMargin)



          //todo 3.8.商品下单次数
           spark.sql(
            """
              |select
              |      t.date_key,
              |      t.__app_version,
              |      t.__lib,
              |      count(1) as product_ordercomplete_cnt
              |from
              |      trace_event_no_visitor t
              |where event = 'exposurce_product'
              |      pageid = 'OrderComplete'
              |group by
              |       t.event_day,t.date_key,t.__app_version,t.__lib
              |""".stripMargin)
  }
}
