package No07_案例.demo01;

import No07_案例.Bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


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


        SingleOutputStreamOperator<Integer> res = filter.keyBy(new KeySelector<UserBehavior, String>() {
            @Override
            public String getKey(UserBehavior value) throws Exception {
                return value.getBehavior();
            }
        }).map(new RichMapFunction<UserBehavior, Integer>() {

            MapState<Long, String> set;

            @Override
            public void open(Configuration parameters) throws Exception {
                set = getRuntimeContext().getMapState(
                        new MapStateDescriptor<Long, String>(
                                "map",
                                Long.class,
                                String.class
                        )
                );
            }

            @Override
            public Integer map(UserBehavior value) throws Exception {
                set.put(value.getUserId(), "");
                return Lists.newArrayList(set.keys().iterator()).size();
            }
        });

        res.print("res");
        env.execute();
    }
}
