package app.dwm;

import Bean.OrderDetail;
import Bean.OrderInfo;
import Bean.OrderWide;
import Function.DimAsyncFunction;
import Util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 *  订单宽表
 */
public class OrderWideApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 1.获取环境
        env.setParallelism(1);//生产环境中和kafka分区数宝池一致

        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000l);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(10000);


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,1000));


        //todo 2.读取kafka 订单流、订单明细流  转为javaBean 设置watermark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String group_id = "order_wide_group";
        String orderWideSinkTopic = "dwd_order_wide";
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(KafkaUtil.getKafkaConsumer(orderInfoSourceTopic, group_id))
                .map(x -> {
                    OrderInfo orderInfo = JSON.parseObject(x, OrderInfo.class);
                    //根据create_time 字段重新设置几个字段
                    String create_time = orderInfo.getCreate_time();
                    String[] dateTimeArr = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimeArr[0]);
                    orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());
                    return orderInfo;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long l) {
                                return orderInfo.getCreate_ts();
                            }
                        }));

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(KafkaUtil.getKafkaConsumer(orderDetailSourceTopic, group_id))
                .map(x -> {
                    OrderDetail orderDetail = JSON.parseObject(x, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());

                    return orderDetail;
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        }));


        //todo 3.两个事实表双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))//生产环境中给的时间是最大延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context context, Collector<OrderWide> collector) throws Exception {
                        collector.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        //todo 4.关联维度信息
        //维表都存在Hbase中，在dwd_BaseDBAPP中进行的分流
        //有六个维度要关联 假设所有数据都在redis缓存了
        //每条事实数据进来后关联一个维度要1ms,那么就是5ms  也就是单并行度每秒钟处理200条
        //效率很低，如果数据量大了 会反压
        //再优化: 高峰期数据量1000-2000条 放在redis本来是够了
        orderWideDS.map(orderWide -> {
            //关联用户维度
            //根据user_id查询phoenix用户信息
            //将用户信息补充到orderWide中
            //关联每个维度都是用id主键查询hbase ，所以可以封装到工具类
            Long user_id = orderWide.getUser_id();

            //返回结果
            return orderWide;
        });

        // todo 4.2 异步IO方式
        //关联省份
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setProvince_name(jsonObject.getString("name"));
                        orderWide.setProvince_area_code(jsonObject.getString("area_code"));
                        orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                    }
                },
                60,
                TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("sku_name"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS);


        //todo 5.将数据写到kafka
        orderWideWithSkuDS.map(JSONObject::toJSONString)
                .addSink(KafkaUtil.getKafkaProducer(orderWideSinkTopic));

        //todo 6.执行任务
        env.execute();
    }
}
