package No07_案例.demo01;

import No07_案例.Bean.MarketingUserBehavior;
import No07_案例.Source.AppSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _04_market_app {
    // 统计 （行为，次数）
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MarketingUserBehavior> source = env.addSource(new AppSourceFunction());

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return new Tuple2<>(value.getBehavior(), 1);
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);

        sum.print();

        env.execute();

    }
}
