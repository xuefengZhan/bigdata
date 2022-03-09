package No07_案例.demo01;

import No07_案例.Bean.PageViewCount;
import No07_案例.Bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

public class _06_PV_双重聚合 {
    //用户ID、商品ID、商品类目ID、行为类型和时间戳
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("mn 'day");


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


        //todo 1.加随机数 离散
        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = userBehaviorSingleOutputStreamOperator.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior userBehavior) throws Exception {
                return new Tuple2<>("pv_" +  new Random().nextInt(8), 1);
            }
        });


        KeyedStream<Tuple2<String, Integer>, String> keyedStream = pv.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> t) throws Exception {
                return t.f0;
            }
        });

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.timeWindow(Time.hours(1));

        //todo 2.聚合获取窗口信息
        SingleOutputStreamOperator<PageViewCount> aggregate = window.aggregate(new MyAgg(), new MyWin() {
        });


        //todo 3.根据窗口重新聚合
        KeyedStream<PageViewCount, String> pageViewCountStringKeyedStream = aggregate.keyBy(new KeySelector<PageViewCount, String>() {
            @Override
            public String getKey(PageViewCount pageViewCount) throws Exception {
                return pageViewCount.getTime();
            }
        });


      //todo 4.状态编程  如果不用状态编程 而是直接聚合，一个窗口分散在8个并行度，每一次聚合都要输出一次会导致输出8次只有最后一次是全量数据
        SingleOutputStreamOperator<PageViewCount> res = pageViewCountStringKeyedStream.process(new KeyedProcessFunction<String, PageViewCount, PageViewCount>() {

            //用来装窗口中各个并行度的数据
            private ListState<PageViewCount> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(
                        new ListStateDescriptor<PageViewCount>(
                                "list-state",
                                PageViewCount.class
                        )
                );
            }

            @Override
            public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
                listState.add(value);


                String time = value.getTime();//窗口关闭时间
                long ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime();
                ctx.timerService().registerEventTimeTimer(ts + 1); //窗口关闭时间+1
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, PageViewCount, PageViewCount>.OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {

                int cnt = 0;
                //取出状态中的数据
                Iterator<PageViewCount> iterator = listState.get().iterator();
                while (iterator.hasNext()) {
                    PageViewCount next = iterator.next();
                    cnt += next.getCount();
                }

                out.collect(new PageViewCount("pv", new Timestamp(timestamp - 1).toString(), cnt));

                listState.clear();
            }
        });

        res.print();

        env.execute();

    }

    //windowFunc可以拿到key信息，这里Acc和Out只用value即可
    public static class MyAgg implements AggregateFunction<Tuple2<String, Integer>,Integer,Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer>s1, Integer i) {
            return i+1;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return integer + acc1;
        }
    }

    public static class MyWin implements WindowFunction<Integer, PageViewCount,String,TimeWindow>{
        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<PageViewCount> out) throws Exception {
            //提取窗口信息
            long end = window.getStart();
            out.collect(new PageViewCount("pv",new Timestamp(end).toString(),input.iterator().next()));
        }
    }
}
