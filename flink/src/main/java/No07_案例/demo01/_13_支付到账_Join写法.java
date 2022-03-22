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
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 *  双流join intervaljoin 实现inner join
 *
 *
 */

public class _13_支付到账_Join写法 {
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


        //todo intervalJoin 只能作用在两个KeyedStream上
        // between : param1 : 被等待时长  param2:等待时长
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = orderEventDS.keyBy(new KeySelector<OrderEvent, String>() {
                    @Override
                    public String getKey(OrderEvent orderEvent) throws Exception {
                        return orderEvent.getTxId();
                    }
                }).intervalJoin(txDS.keyBy(new KeySelector<TxEvent, String>() {
                    @Override
                    public String getKey(TxEvent txEvent) throws Exception {
                        return txEvent.getTxID();
                    }
                })).between(Time.seconds(-5), Time.seconds(10))
                .process(new PayReceiptJoinProcessFunc());


        result.print();

        env.execute();


    }

    private static class PayReceiptJoinProcessFunc extends ProcessJoinFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>{

        @Override
        public void processElement(OrderEvent orderEvent, TxEvent txEvent, ProcessJoinFunction<OrderEvent, TxEvent, Tuple2<OrderEvent, TxEvent>>.Context context, Collector<Tuple2<OrderEvent, TxEvent>> collector) throws Exception {
            collector.collect(new Tuple2<>(orderEvent,txEvent));
        }
    }


}
