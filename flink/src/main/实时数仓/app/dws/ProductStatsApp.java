package app.dws;

import Bean.OrderWide;
import Bean.PaymentWide;
import Bean.ProductStats;
import Util.DateTimeUtil;
import Util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import common.GmallConstant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */

        //TODO 1.从Kafka中获取数据流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";
        FlinkKafkaConsumer<String> pageViewSource = KafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> orderWideSource = KafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> paymentWideSource = KafkaUtil.getKafkaConsumer(paymentWideSourceTopic, groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSouce = KafkaUtil.getKafkaConsumer(favorInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> cartInfoSource = KafkaUtil.getKafkaConsumer(cartInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> refundInfoSource = KafkaUtil.getKafkaConsumer(refundInfoSourceTopic, groupId);
        FlinkKafkaConsumer<String> commentInfoSource = KafkaUtil.getKafkaConsumer(commentInfoSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);


        //TODO 2.对获取的流数据进行结构的转换
        //2.1 埋点数据 从：dwd_page_log 转换曝光及页面流数据
        SingleOutputStreamOperator<ProductStats> pageAndDispStatsDstream = pageViewDStream.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String json, Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(json);
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        String pageId = pageJsonObj.getString("page_id");
                        if (pageId == null) {
                            System.out.println(jsonObj);
                        }
                        Long ts = jsonObj.getLong("ts");
                        //如果pageId为商品详情
                        if (pageId.equals("good_detail")) {
                            Long skuId = pageJsonObj.getLong("item");
                            ProductStats productStats = ProductStats.builder().sku_id(skuId).
                                    click_ct(1L).ts(ts).build();
                            out.collect(productStats);
                        }
                        JSONArray displays = jsonObj.getJSONArray("display");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                if (display.getString("item_type").equals("sku_id")) {
                                    Long skuId = display.getLong("item");
                                    ProductStats productStats = ProductStats.builder()
                                            .sku_id(skuId).display_ct(1L).ts(ts).build();
                                    out.collect(productStats);
                                }
                            }
                        }
                    }
                });

        //2.2转换下单流数据
        SingleOutputStreamOperator<ProductStats> orderWideStatsDstream = orderWideDStream.map(
                json -> {
                    OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
                    System.out.println("orderWide:===" + orderWide);
                    String create_time = orderWide.getCreate_time();
                    Long ts = DateTimeUtil.toTs(create_time);
                    return ProductStats.builder().sku_id(orderWide.getSku_id())
                            .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                            .order_sku_num(orderWide.getSku_num())
                            .order_amount(orderWide.getSplit_total_amount()).ts(ts).build();
                });

        //2.3转换收藏流数据
        SingleOutputStreamOperator<ProductStats> favorStatsDstream = favorInfoDStream.map(
                json -> {
                    JSONObject favorInfo = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(favorInfo.getString("create_time"));
                    return ProductStats.builder().sku_id(favorInfo.getLong("sku_id"))
                            .favor_ct(1L).ts(ts).build();
                });

        //2.4转换购物车流数据
        SingleOutputStreamOperator<ProductStats> cartStatsDstream = cartInfoDStream.map(
                json -> {
                    JSONObject cartInfo = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(cartInfo.getString("create_time"));
                    return ProductStats.builder().sku_id(cartInfo.getLong("sku_id"))
                            .cart_ct(1L).ts(ts).build();
                });

        //2.5转换支付流数据
        SingleOutputStreamOperator<ProductStats> paymentStatsDstream = paymentWideDStream.map(
                json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder().sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts).build();
                });

        //2.6转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundStatsDstream = refundInfoDStream.map(
                json -> {
                    JSONObject refundJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(refundJsonObj.getLong("sku_id"))
                            .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(
                                    new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                            .ts(ts).build();
                    return productStats;
                });

        //2.7转换评价流数据
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDstream = commentInfoDStream.map(
                json -> {
                    JSONObject commonJsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                    Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(commonJsonObj.getLong("sku_id"))
                            .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                    return productStats;
                });

        //TODO 3.把统一的数据结构流合并为一个流
        DataStream<ProductStats> productStatDetailDStream = pageAndDispStatsDstream.union(
                orderWideStatsDstream, cartStatsDstream,
                paymentStatsDstream, refundStatsDstream,favorStatsDstream,
                commonInfoStatsDstream);

        productStatDetailDStream.print("after union:");

        //TODO 4.设定事件时间与水位线
        SingleOutputStreamOperator<ProductStats> productStatsWithTsStream =
                productStatDetailDStream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ProductStats>forMonotonousTimestamps().withTimestampAssigner(
                                (productStats, recordTimestamp) -> {
                                    return productStats.getTs();
                                })
                );




        env.execute();

    }
}
