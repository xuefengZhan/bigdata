package No07_案例.demo01;

import No07_案例.Bean.UserBehavior;
import No07_案例.Bean.UserViewCount;
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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

public class _07_UV_全窗口聚合 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\Data\\UserBehavior.csv");


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
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = userBehaviorDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000l;
            }
        });


        //pv的进入一个key
        WindowedStream<UserBehavior, String, TimeWindow> window = userBehaviorSingleOutputStreamOperator.keyBy(new KeySelector<UserBehavior, String>() {

            @Override
            public String getKey(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior();
            }
        }).timeWindow(Time.hours(1));

        SingleOutputStreamOperator<UserViewCount> res = window.process(new UVWindowFunc());


        res.print();
        env.execute();
    }


    //todo 全窗口函数
    public static class UVWindowFunc extends ProcessWindowFunction<UserBehavior, UserViewCount,String,TimeWindow>{


       HashSet<Long> set = new HashSet<>();


        @Override
        public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserViewCount> out) throws Exception {
            Iterator<UserBehavior> iterator = elements.iterator();
            while(iterator.hasNext()){
                set.add(iterator.next().getUserId());
            }

            out.collect(new UserViewCount("uv",new Timestamp(context.window().getEnd()).toString(),set.size()));


            set.clear();
        }
    }
}
