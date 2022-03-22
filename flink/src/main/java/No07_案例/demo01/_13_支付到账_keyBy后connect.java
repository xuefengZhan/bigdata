package No07_案例.demo01;

import No07_案例.Bean.OrderEvent;
import No07_案例.Bean.TxEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *  双流join 可以用Connect算子
 */

public class _13_支付到账_keyBy后connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> order = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\Data\\OrderLog.csv");
        DataStreamSource<String> receipt = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\Data\\ReceiptLog.csv");


        //todo 1.两个流转成JavaBean  提取时间戳
        SingleOutputStreamOperator<OrderEvent> orderEventDS = order.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String s, Collector<OrderEvent> collector) throws Exception {
                String[] fields = s.split(",");
                OrderEvent orderEvent = new OrderEvent(Long.parseLong(fields[0]),
                        fields[1],
                        fields[2],
                        Long.parseLong(fields[3]));

                if ("pay".equals(orderEvent.getEventType())) {
                    collector.collect(orderEvent);
                }
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent orderEvent) {
                return orderEvent.getEventTime() * 1000;
            }
        });

        SingleOutputStreamOperator<TxEvent> txDS = receipt.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String s) throws Exception {
                String[] fields = s.split(",");
                return new TxEvent(fields[0], fields[1], Long.parseLong(fields[2]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TxEvent>() {
            @Override
            public long extractAscendingTimestamp(TxEvent txEvent) {
                return txEvent.getEventTime() * 1000;
            }
        });


        KeyedStream<OrderEvent, String> orderEventKeyedDS = orderEventDS.keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent orderEvent) throws Exception {
                return orderEvent.getTxId();
            }
        });

        KeyedStream<TxEvent, String> txEventKeyedStream = txDS.keyBy(new KeySelector<TxEvent, String>() {
            @Override
            public String getKey(TxEvent txEvent) throws Exception {
                return txEvent.getTxID();
            }
        });


        //todo 3.连接两个流 connect 实现
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderEventKeyedDS.connect(txEventKeyedStream).process(new OrderReceiptKeyProFunc());

        result.print();
        result.getSideOutput(new OutputTag<String>("支付数据没到"){}).print();
        result.getSideOutput(new OutputTag<String>("支付了 没到账"){}).print();

        env.execute();


    }

    private static class OrderReceiptKeyProFunc extends KeyedCoProcessFunction<String,OrderEvent,TxEvent,Tuple2<OrderEvent,TxEvent>>{
        ValueState<OrderEvent> orderValue;
        ValueState<TxEvent> TxValue;

        // 只要涉及删除定时器的都需要保存定时器时间
        ValueState<Long> ts;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderValue = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>(
                    "orderState",
                    OrderEvent.class
            ));

            TxValue = getRuntimeContext().getState(new ValueStateDescriptor<TxEvent>(
                    "TxState",
                    TxEvent.class
            ));

            ts = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>(
                            "timestamp",
                            long.class
                    )
            );
        }


        //todo 处理订单数据

        @Override
        public void processElement1(OrderEvent orderEvent, KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.Context context, Collector<Tuple2<OrderEvent, TxEvent>> collector) throws Exception {

            TxEvent Tx = TxValue.value();

            // 到账数据还没到，保存状态 注册定时器 10s
            if(Tx == null){
                orderValue.update(orderEvent);
                long timestamp = (orderEvent.getEventTime() + 10) * 1000;
                context.timerService().registerEventTimeTimer( timestamp );

                ts.update(timestamp);

            }else{
                // 到账数据已经到
                // 1.输出
                collector.collect(new Tuple2<>(orderEvent,Tx));
                // 2.删除定时器
                context.timerService().deleteEventTimeTimer(ts.value());
                // 3.清空状态
                TxValue.clear();
                ts.clear();
            }
        }


        // todo 处理到账数据
        @Override
        public void processElement2(TxEvent txEvent, KeyedCoProcessFunction<String, OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.Context context, Collector<Tuple2<OrderEvent, TxEvent>> collector) throws Exception {
            OrderEvent order = orderValue.value();
            //下单数据没到
            if(order == null){
                TxValue.update(txEvent);
                long time =  ( txEvent.getEventTime() + 5) * 1000;
                context.timerService().registerEventTimeTimer(time);
                ts.update(time);
            }else{
                collector.collect(new Tuple2<>(order,txEvent));
                // 2.删除定时器
                context.timerService().deleteEventTimeTimer(ts.value());
                // 3.清空状态
                orderValue.clear();
                ts.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            OrderEvent order = orderValue.value();
            TxEvent tx = TxValue.value();

            //支付的数据超时了
            if(order == null){
                ctx.output(
                        new OutputTag<String>("支付数据没到"){},
                        tx.getTxID() + "收到钱了 但是没收到支付数据"
                );
            }else{
                ctx.output(
                        new OutputTag<String>("支付了 没到账"){},
                        order.getTxId() + "支付了 但没到账"
                );
            }
        }
    }

}
