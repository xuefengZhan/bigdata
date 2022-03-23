package app.dwm;

import Bean.OrderWide;
import Bean.PaymentInfo;
import Bean.PaymentWide;
import Util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 1.读取Kafka主题数据  dwd_payment_info  dwm_order_wide
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        DataStreamSource<String> paymentKafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(orderWideSourceTopic, groupId));

        //todo 2.将数据转换为JavaBean并提取时间戳生成WaterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS
                .map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！！");
                                }
                            }
                        }));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS
                .map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                try {
                                    return sdf.parse(element.getCreate_time()).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    throw new RuntimeException("时间格式错误！！");
                                }
                            }
                        }));

        //todo 3.双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(5))  //支付被等待15min  支付等待5s
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        paymentWideDS.map(JSON::toJSONString).addSink(KafkaUtil.getKafkaProducer(paymentWideSinkTopic));

        env.execute();

    }
}
