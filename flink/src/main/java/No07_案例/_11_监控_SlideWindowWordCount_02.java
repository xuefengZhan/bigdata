package No07_案例;

import No07_案例.Bean.AdWindowCnt;
import No07_案例.Bean.AdsClickLog;
import akka.japi.tuple.Tuple3;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * 每隔5s 1h内的 按照省份  统计广告点击次数
 * 如果一天内，如果对同一个广告点击超过一定限额（比如100次），应该把该用户加入黑名单并报警，此后其点击行为不应该再统计
 * 报警走侧输出流
 *
 *   (（天，用户，广告），cnt)
 *
 */
public class _11_监控_SlideWindowWordCount_02 {
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



        // 黑名单过滤   key = 用户+广告
        SingleOutputStreamOperator<AdsClickLog> filteredDs = adsClickDS.keyBy(new KeySelector<AdsClickLog, String>() {
            @Override
            public String getKey(AdsClickLog value) throws Exception {
                return value.getUserId() + "_" + value.getAdId();
            }
        }).process(new BlackListProcessFunc(100));

        WindowedStream<AdsClickLog, String, TimeWindow> window = filteredDs.keyBy(new KeySelector<AdsClickLog, String>() {
            @Override
            public String getKey(AdsClickLog value) throws Exception {
                return value.getCity();
            }
        }).timeWindow(Time.hours(1), Time.seconds(5));


        SingleOutputStreamOperator<AdWindowCnt> aggregate = window.aggregate(new MyAgg(), new MyWin());


        aggregate.print();
        filteredDs.getSideOutput(new OutputTag<String>("side"){}).print("warn");

        env.execute();

    }


    private static class BlackListProcessFunc extends KeyedProcessFunction<String,AdsClickLog,AdsClickLog>{

        private final int maxClickTimes;
        public BlackListProcessFunc(int maxClickTimes){
            this.maxClickTimes = maxClickTimes;
        }

        ValueState<Integer> count;
        ValueState<Boolean> isBlacked;

        @Override
        public void open(Configuration parameters) throws Exception {
            count = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>(
                            "cnt",
                            Integer.class
                            ,0
                    )
            );

            isBlacked = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>(
                            "blacked",
                            Boolean.class,
                            false
                    )
            );
        }


        @Override
        public void processElement(AdsClickLog value, Context ctx, Collector<AdsClickLog> out) throws Exception {
            if(count.value() == 0){
                //获取第二天凌晨的时间，注册定时器 value.getTimestamp()是秒
                count.update(count.value() + 1 );
                long zeroOclock = ( (value.getTimestamp() / (24 * 3600) + 1) * 24  * 3600 * 1000 - 8*3600*1000) ;
                //System.out.println(new Timestamp(zeroOclock));
                ctx.timerService().registerProcessingTimeTimer( zeroOclock );
                out.collect(value);
            }else{

                count.update(count.value() + 1 );

                if(count.value() >= maxClickTimes){
                    //只做一次输出
                    if(!isBlacked.value()){
                        ctx.output(new OutputTag<String>("side"){},"用户 " + value.getUserId()+"已经点击广告" + value.getAdId() + "超过一百次了!");
                    }
                    isBlacked.update(true);
                }else{
                    //直接输出
                    out.collect(value);
                }
            }
        }

//        @Override
//        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdsClickLog> out) throws Exception {
//            count.update(0);
//            isBlacked.update(false);
//        }
        //系统时间早就过了2017年，所以这里把定时器注了
    }
    private static class MyAgg implements AggregateFunction<AdsClickLog, Integer,Integer> {


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
