package No07_案例;

import No07_案例.Bean.UserBehavior;
import No07_案例.Bean.UserViewCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

public class _07_UV_全窗口聚合2 {
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

        SingleOutputStreamOperator<UserViewCount> res = window.
                trigger(new MyTrigger())
                .process(new UVWindowFunc());


        res.print();
        env.execute();
    }

    /**
     * 自定义触发器，告诉窗口什么时候计算 什么时候输出结果
     * 计算使用的是后续的处理函数
     *
     * No action is taken on the window.
          CONTINUE(false, false),

     * 来一条计算一条 并将结果写出
          FIRE_AND_PURGE(true, true),

     * 来一条只做计算 不写出
           FIRE(true, false),

     * 来一条不计算 直接写出
         PURGE(false, true);
     */
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        //元素来了
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        //处理时间到了
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        //事件时间到了
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    //todo 全窗口函数   由于前面调用了自定义触发器来一条计算一条
    // 因此这里全窗口迭代器中只有一条数据
    public static class UVWindowFunc extends ProcessWindowFunction<UserBehavior, UserViewCount,String,TimeWindow>{

       //初始化redis 连接  和bloomFilter
        private Jedis jedis;

        private MyBloomFilter filter;


        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("Hadoop102",6379);
           //一亿条数据大概 一条大概20byte
            // 20 * 10^8byte  =  2G
            // 一亿条数据 减少hash碰撞 用10亿的容量
            // 10亿bit = 10^9 bit = 10 / 8 * 10^8 byte = 1.2 * 10^5 k =1.2 * 10 ^ 2 M  不到200M
            //10亿 = 2 ^ 30
            filter = new MyBloomFilter(1 << 30);


        }

        @Override
        public void process(String s, Context context, Iterable<UserBehavior> elements, Collector<UserViewCount> out) throws Exception {
            UserBehavior userBehavior = elements.iterator().next();


            //窗口时间作为key
            String windEnd = new Timestamp(context.window().getEnd()).toString();
            String key = "BitMap_" + windEnd;

            //查询当前uid是否已经存在当前bitmap中
            long offset = filter.getOffset(userBehavior.getUserId().toString());
            Boolean exist = jedis.getbit(key, offset);

            //如果不存在放进去，并且累加UV
            if(!exist){
                jedis.setbit(key,offset,true);//将该offset置为1
            }

        }
    }

    public static class MyBloomFilter{
        private long cap;//容量

        public MyBloomFilter(long cap){
            this.cap = cap;
        }

        //hash
        public long getOffset(String value){
            long result = 0l;

            for (char c : value.toCharArray()) {
                result += result * 31 + c;
            }

            return result & (cap - 1);
        }
    }
}
