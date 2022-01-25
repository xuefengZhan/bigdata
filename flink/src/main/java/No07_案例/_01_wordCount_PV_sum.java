package No07_案例;

import No07_案例.Bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class _01_wordCount_PV_sum {

    //需求：统计pv次数
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

//        userBehaviorDS.print();

        // 3.2 过滤 并且 转换成（pv，1）
        SingleOutputStreamOperator<Tuple2<String, Integer>>  pvAndOneDS = userBehaviorDS.flatMap(new FlatMapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(UserBehavior value, Collector<Tuple2<String, Integer>> out) throws Exception {
                if ("pv".equals(value.getBehavior())) {
                    out.collect(new Tuple2<>("pv", 1));
                }
            }
        });



        // 3.3 按照维度分组：pv行为
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = pvAndOneDS.keyBy(0);

        // 3.4 求和：

        SingleOutputStreamOperator<Tuple2<String, Integer>> pvDS = keyedStream.sum(1);
        pvDS.print();

        env.execute();


    }
}
