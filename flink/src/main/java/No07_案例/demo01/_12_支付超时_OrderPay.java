package No07_案例.demo01;

import No07_案例.Bean.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class _12_支付超时_OrderPay {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\Data\\OrderLog.csv");

        SingleOutputStreamOperator<OrderEvent> orderEventSingleOutputStreamOperator = source.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String s) throws Exception {
                String[] fields = s.split(",");
                return new OrderEvent(Long.parseLong(fields[0]),
                        fields[1],
                        fields[2],
                        Long.parseLong(fields[3]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent orderEvent) {
                return orderEvent.getEventTime() * 1000;
            }
        });

        //todo 1.按照订单id分组
        KeyedStream<OrderEvent, Long> orderEventLongKeyedStream = orderEventSingleOutputStreamOperator.keyBy(
                new KeySelector<OrderEvent, Long>() {
                    @Override
                    public Long getKey(OrderEvent orderEvent) throws Exception {
                        return orderEvent.getOrderId();
                    }
                }
        );

        //todo 2.状态 + 定时器
        SingleOutputStreamOperator<String> result = orderEventLongKeyedStream.process(new OrderPayProcessFunc(15));

        result.print();
        result.getSideOutput(new OutputTag<String>("支付超时 或者 没有订单信息"){}).print("no create");
        result.getSideOutput(new OutputTag<String>("no pay"){}).print("no pay");

        env.execute();


    }
        private static class OrderPayProcessFunc extends KeyedProcessFunction<Long, OrderEvent, String> {

            private int interval;

            public OrderPayProcessFunc(int interval) {
                this.interval = interval;
            }


            //存储订单 如果支付了，清空
            ValueState<OrderEvent> valueState;
            //存储定时器时间，如果15分钟内 支付数据来了 则注销定时器
            ValueState<Long> timestamp;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(
                        new ValueStateDescriptor<OrderEvent>(
                                "value",
                                OrderEvent.class
                        )
                );

                timestamp = getRuntimeContext().getState(
                        new ValueStateDescriptor<Long>(
                                "ts",
                                Long.class
                        )
                );
            }

            @Override
            public void processElement(OrderEvent orderEvent,  Context context, Collector<String> collector) throws Exception {
                //如果是创建订单事件，则注册定时器
                // 将订单存入状态
                if(orderEvent.getEventType().equals("create")){
                    valueState.update(orderEvent);
                    context.timerService().registerEventTimeTimer(orderEvent.getEventTime() * 1000 + interval * 60 * 1000);
                    timestamp.update(orderEvent.getEventTime() * 1000 + interval * 60 * 1000);
                }else if(orderEvent.getEventType().equals("pay")){
                    //支付数据  清空状态
                    OrderEvent order = valueState.value();

                    //支付数据可能在订单数据之前
                    if(order == null){
                        context.output(new OutputTag<String>("支付超时 或者 没有订单信息"){},
                          orderEvent.getOrderId() + "支付数据到了但是下单数据没到" );
                    }else{
                        collector.collect(
                                order.getOrderId() + "Create at " +
                                        order.getEventTime() + " pay at" + orderEvent.getEventTime()
                        );

                        // 删除定时器
                        context.timerService().deleteEventTimeTimer(this.timestamp.value());

                        valueState.clear();
                        timestamp.clear();
                    }

                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                // 定时器响了 有创建但是超时未支付
                OrderEvent order = valueState.value();
                ctx.output(
                        new OutputTag<String>("no pay"){},
                        order.getOrderId() + "下单了 但是没支付"
                );


                valueState.clear();
                this.timestamp.clear();
            }
        }

}
