package No07_案例;

import No07_案例.Bean.ItemWindowCnt;
import No07_案例.Bean.UserBehavior;
import javafx.scene.input.DataFormat;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 需求： 每隔5min输出最近1小时内点击量最多的前N个商品
 */
public class _08_SlideWindowTopN_顺序数据 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = userBehaviorDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000L;
            }
        });

        //用户ID、商品ID、商品类目ID、行为类型和时间戳
        //按照商品id keyBy
        KeyedStream<UserBehavior, Long> keyedByItemStream = userBehaviorSingleOutputStreamOperator.keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior value) throws Exception {
                return value.getItemId();
            }
        });

        //todo 聚合 select item,window,count(*) from xxx group by item,window;

        //todo 1、 select item,window,count(*) from xxx group by item;
        WindowedStream<UserBehavior, Long, TimeWindow> window = keyedByItemStream.timeWindow(Time.hours(1), Time.minutes(5));

        SingleOutputStreamOperator<ItemWindowCnt> aggregate = window.aggregate(new MyaggFunc(), new MyWinFunc());

        //todo 2、 select item,window,count(*) from xxx group by item，window;
        KeyedStream<ItemWindowCnt, String> itemWindowCntStringKeyedStream = aggregate.keyBy(new KeySelector<ItemWindowCnt, String>() {
            @Override
            public String getKey(ItemWindowCnt value) throws Exception {
                return value.getTimeStamp();
            }
        });

        SingleOutputStreamOperator<String> res = itemWindowCntStringKeyedStream.process(new MyKeyFunc(5));

        res.print();
        env.execute();


    }
    //key 和 window 在windowFunction中都可以拿到，所以这里只要cnt即可
    public static class MyaggFunc implements AggregateFunction<UserBehavior, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(UserBehavior value, Integer accumulator) {
            return accumulator +1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    public static class MyWinFunc implements WindowFunction<Integer, ItemWindowCnt,Long,TimeWindow> {


        @Override
        public void apply(Long key, TimeWindow window, Iterable<Integer> input, Collector<ItemWindowCnt> out) throws Exception {
            long end = window.getEnd();
            Timestamp ts = new Timestamp(end);

            Integer cnt = input.iterator().next();
            out.collect(new ItemWindowCnt(key,ts.toString(),cnt));
        }
    }


    public static class MyKeyFunc extends KeyedProcessFunction<String, ItemWindowCnt, String> {

        private final int size;

        ListState<ItemWindowCnt> listState;



        public MyKeyFunc(int size){
            this.size = size;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemWindowCnt>(
                            "list",
                            ItemWindowCnt.class
                    )
            );
        }

        @Override
        public void processElement(ItemWindowCnt value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);
            //1.字符串格式化为Data
            String timeStamp = value.getTimeStamp();
            SimpleDateFormat form = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date= form .parse( timeStamp);
            //2.Data转为long
            long time = date.getTime();
            ctx.timerService().registerEventTimeTimer(time  + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<ItemWindowCnt> iterator = listState.get().iterator();
            ArrayList<ItemWindowCnt> itemWindowCnts = Lists.newArrayList(iterator);
            itemWindowCnts.sort((x,y) -> { return y.getCount() - x.getCount();});

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String ts = simpleDateFormat.format(new Date(timestamp));

            StringBuilder sb = new StringBuilder();
            sb.append("====窗口  ").append(ts).append("的top").append(size).append("为\n");

            for (int i = 0; i < Math.min(itemWindowCnts.size(), size); i++) {
                ItemWindowCnt itemWindowCnt = itemWindowCnts.get(i);
                sb.append("[ItemId = ").append(itemWindowCnt.getItemId()).append(",count = ").append(itemWindowCnt.getCount()).append("]\n");
            }

            out.collect(sb.toString());
            listState.clear();

            Thread.sleep(1000);
        }
    }
}
