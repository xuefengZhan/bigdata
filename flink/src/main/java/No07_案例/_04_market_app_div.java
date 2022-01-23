package No07_案例;

import No07_案例.Bean.MarketingUserBehavior;
import No07_案例.Source.AppSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _04_market_app_div {
    // 统计 （（渠道，行为），次数）
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MarketingUserBehavior> source = env.addSource(new AppSourceFunction());

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tuple = source.map(new MapFunction<MarketingUserBehavior, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(MarketingUserBehavior m) throws Exception {
                return new Tuple3<>(m.getChannel(), m.getBehavior(), 1);
            }
        });

        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> keyedStream = tuple.keyBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Integer> s) throws Exception {
                return new Tuple2<>(s.f0, s.f1);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> res = keyedStream.map(new RichMapFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>>() {
            ValueState<Integer> cnt;

            @Override
            public void open(Configuration parameters) throws Exception {
                cnt = getRuntimeContext().getState(
                        new ValueStateDescriptor<Integer>(
                                "cnt-state",
                                Integer.class,
                                0
                        )
                );
            }

            @Override
            public Tuple3<String, String, Integer> map(Tuple3<String, String, Integer> s) throws Exception {
                Integer lastCnt = cnt.value();
                cnt.update(lastCnt + 1);
                return new Tuple3<>(s.f0, s.f1, cnt.value());

            }
        });


        res.print();

        env.execute();

    }
}
