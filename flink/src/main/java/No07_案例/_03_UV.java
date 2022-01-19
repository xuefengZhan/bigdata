package No07_案例;

import No07_案例.Bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _03_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\Data\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = source.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                long userId = Long.parseLong(fields[0]);
                long itemId = Long.parseLong(fields[1]);
                int categoryId = Integer.parseInt(fields[2]);
                String behavior = fields[3];
                long timestamp = Long.parseLong(fields[4]);

                return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
            }
        });


        SingleOutputStreamOperator<UserBehavior> filter = userBehaviorDS.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        KeyedStream<UserBehavior, Long> keyedStream = filter.keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior value) throws Exception {
                return value.getUserId();
            }
        });


        SingleOutputStreamOperator<Integer> res = keyedStream .flatMap(new RichFlatMapFunction<UserBehavior, Integer>() {

            MapState<Long, Integer> set;
            ValueState<Integer> cnt;

            @Override
            public void open(Configuration parameters) throws Exception {
                set = getRuntimeContext().getMapState(
                        new MapStateDescriptor<Long, Integer>(
                                "set-state",
                                Long.class,
                                Integer.class
                        )
                );

                cnt = getRuntimeContext().getState(
                        new ValueStateDescriptor<Integer>(
                                "cnt-state",
                                Integer.class,
                                0
                        )
                );
            }

            @Override
            public void flatMap(UserBehavior value, Collector<Integer> out) throws Exception {
                if (!set.contains(value.getUserId())) {
                    set.put(value.getUserId(), 1);
                    cnt.update(cnt.value() + 1);
                    out.collect(cnt.value());
                }
            }
        });

        res.print("res1");

        KeyedStream<Integer, Integer> keyedStream1 = res.keyBy(new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                return 0;
            }
        });

        SingleOutputStreamOperator<Integer> res2 = keyedStream1.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        });

        res2.print("res2");
        env.execute();
    }
}
