package No07_案例;

import No07_案例.Bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class _02_PV {

    //用户ID、商品ID、商品类目ID、行为类型和时间戳
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



        // 3.2 过滤 并且 转换成（pv，1）
        SingleOutputStreamOperator<UserBehavior>  pvAndOneDS = userBehaviorDS.flatMap(new FlatMapFunction<UserBehavior,UserBehavior>() {
            @Override
            public void flatMap(UserBehavior value, Collector<UserBehavior> out) throws Exception {
                if ("pv".equals(value.getBehavior())) {
                    out.collect(value);
                }
            }
        });



        // 3.3 只有pv的数据了，keyBy走个过场即可
        KeyedStream<UserBehavior,String>  keyedStream = pvAndOneDS.keyBy(x->"pv");

        // 3.4 求和：
        SingleOutputStreamOperator<Integer> res = keyedStream.process(new KeyedProcessFunction<String, UserBehavior, Integer>() {

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
            public void processElement(UserBehavior value, Context ctx, Collector<Integer> out) throws Exception {
                cnt.update(cnt.value() + 1);
                out.collect(cnt.value());
            }
        });


        res.print();


        env.execute();


    }
}
