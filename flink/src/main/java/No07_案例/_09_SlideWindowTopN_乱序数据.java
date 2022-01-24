package No07_案例;

import No07_案例.Bean.UrlLog;
import No07_案例.Bean.UrlWindowCnt;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

/**
 * 需求：每隔五秒钟 输出最近10min内访问量最多的前N个URL
 */
public class _09_SlideWindowTopN_乱序数据 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> source = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\Data\\apache.log");

        SingleOutputStreamOperator<UrlLog> urlLogDS = source.map(new RichMapFunction<String, UrlLog>() {

            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("MM/dd/yyyy:HH:mm:ss");
            }

            @Override
            public UrlLog map(String value) throws Exception {

                String[] fields = value.split(" ");
                String ip = fields[0];
                String userId = fields[1];

                Date date = sdf.parse(fields[3]);
                long ts = date.getTime();

                String method = fields[5];
                String url = fields[6];
                return new UrlLog(ip, userId, ts, method, url);
            }
        });

        SingleOutputStreamOperator<UrlLog> urlLogSingleOutputStreamOperator = urlLogDS.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<UrlLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(UrlLog element) {
                        return element.getTs();
                    }
                }
        );

        SingleOutputStreamOperator<UrlLog> filter = urlLogSingleOutputStreamOperator.filter(x -> "GET".equals(x.getMethod()));


        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple = filter.map(new MapFunction<UrlLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UrlLog value) throws Exception {
                return new Tuple2<>(value.getUrl(), 1);
            }
        });

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = tuple.keyBy(x -> x.f0).
                timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<Tuple2<String,Integer>>("side"){});

        //url window cnt
        SingleOutputStreamOperator<UrlWindowCnt> aggregate = window.aggregate(new MyAgg(), new MyWin());


        SingleOutputStreamOperator<String> res = aggregate.keyBy(UrlWindowCnt::getTimeStamp).process(new MyKeyFun(5));


        res.print();

        env.execute();


    }


    public static class MyAgg implements AggregateFunction<Tuple2<String,Integer>,Integer,Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }


    public static class MyWin implements WindowFunction<Integer, UrlWindowCnt,String,TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, java.lang.Iterable<Integer> input, Collector<UrlWindowCnt> out) throws Exception {
            long end = window.getEnd();
            Date date = new Date(end);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String timestamp = sdf.format(date);

            out.collect(new UrlWindowCnt(key,timestamp,input.iterator().next()));
        }
    }


    public static class MyKeyFun extends KeyedProcessFunction<String, UrlWindowCnt, String> {

        ListState<UrlWindowCnt> list;


        ValueState<Long> closeTime;
        private final int size;


        public MyKeyFun(int size){
            this.size = size;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            list = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlWindowCnt>(
                            "list",
                            UrlWindowCnt.class
                    )
            );


            closeTime = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>(
                            "value",
                            Long.class
                    )
            );
        }

        @Override
        public void processElement(UrlWindowCnt value, Context ctx, Collector<String> out) throws Exception {

            list.add(value);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date parse = sdf.parse(value.getTimeStamp());
            long time = parse.getTime();
            // todo  注册定时器1 ： wm >= window_end 做第一次统一计算写出 不能清空状态
            ctx.timerService().registerEventTimeTimer(time + 1);

            // todo 注册定时器2 ： wm >= window_end + laterness 关闭窗口 清空状态
            ctx.timerService().registerEventTimeTimer(time + 1000 + 60 * 1000 + 1);
            closeTime.update(time + 1000 + 60000 + 1);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Iterator<UrlWindowCnt> iterator = list.get().iterator();
            ArrayList<UrlWindowCnt> arr = Lists.newArrayList(iterator);
            arr.sort((x, y) -> y.getCount() - x.getCount());


            Date date = new Date(timestamp);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String ts = sdf.format(date);

            StringBuilder sb = new StringBuilder();
            sb.append("====窗口  ").append(ts).append("的top").append(size).append("为\n");

            for (int i = 0; i < Math.min(arr.size(), size); i++) {
                UrlWindowCnt urlWindowCnt = arr.get(i);
                sb.append("[Url = ").append(urlWindowCnt.getUrl()).append(",count = ").append(urlWindowCnt.getCount()).append("]\n");
            }

            out.collect(sb.toString());

           if(timestamp == closeTime.value()){
               list.clear();
               return ;
           }

           Thread.sleep(1000);
        }
    }

}
