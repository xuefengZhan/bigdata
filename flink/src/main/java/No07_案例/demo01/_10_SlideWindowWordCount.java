package No07_案例.demo01;

import No07_案例.Bean.AdWindowCnt;
import No07_案例.Bean.AdsClickLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * 每隔5s 1h内的 按照省份  统计广告点击次数
 */
public class _10_SlideWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> source = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\Data\\AdClickLog.csv");
        SingleOutputStreamOperator<AdsClickLog> adsClickDS = source.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] datas = value.split(",");
                return new AdsClickLog(
                        Long.valueOf(datas[0]),
                        Long.valueOf(datas[1]),
                        datas[2],
                        datas[3],
                        Long.valueOf(datas[4]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdsClickLog>() {
            @Override
            public long extractAscendingTimestamp(AdsClickLog element) {
                return element.getTimestamp() * 1000;
            }
        });

        WindowedStream<AdsClickLog, String, TimeWindow> window = adsClickDS.keyBy(new KeySelector<AdsClickLog, String>() {
            @Override
            public String getKey(AdsClickLog value) throws Exception {
                return value.getCity();
            }
        }).timeWindow(Time.hours(1), Time.seconds(5));


        SingleOutputStreamOperator<AdWindowCnt> aggregate = window.aggregate(new MyAgg(), new MyWin());


        KeyedStream<AdWindowCnt, String> keyedStream = aggregate.keyBy(new KeySelector<AdWindowCnt, String>() {
            @Override
            public String getKey(AdWindowCnt value) throws Exception {
                return value.getTime();
            }
        });





        SingleOutputStreamOperator<String> res = keyedStream.process(new KeyedProcessFunction<String, AdWindowCnt, String>() {
            MapState<String, AdWindowCnt> map;

            SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                map = getRuntimeContext().getMapState(
                        new MapStateDescriptor<String, AdWindowCnt>(
                                "map",
                                String.class,
                                AdWindowCnt.class
                        )
                );
            }

            @Override
            public void processElement(AdWindowCnt value, Context ctx, Collector<String> out) throws Exception {
                String time = value.getTime();
                long end = sdf.parse(time).getTime();
                ctx.timerService().registerEventTimeTimer(end + 1);

                map.put(value.getProvince(), value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                Iterator<AdWindowCnt> iterator = map.values().iterator();
                StringBuilder sb = new StringBuilder();
                sb.append("=========窗口 " + sdf.format(new Date(timestamp - 1)) + " 所有省份的广告点击次数 =========\n");

                while (iterator.hasNext()) {
                    sb.append(iterator.next().toString()).append("\n");
                }

                sb.append("=========窗口 " + sdf.format(new Date(timestamp - 1)) + " 所有省份的广告点击次数 =========\n");
                out.collect(sb.toString());
            }
        });


        aggregate.print();

        env.execute();

    }

    private static class MyAgg implements AggregateFunction<AdsClickLog, Integer,Integer>{


        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(AdsClickLog value, Integer accumulator) {
            return accumulator + 1 ;
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

    private static class MyWin implements WindowFunction<Integer,AdWindowCnt,String,TimeWindow> {


        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<AdWindowCnt> out) throws Exception {
                long end = window.getEnd();
                Date date = new Date(end);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String ts = sdf.format(date);

                out.collect(new AdWindowCnt(s,ts,input.iterator().next()));

            }
        }

}
