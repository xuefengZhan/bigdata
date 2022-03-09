package No07_案例.demo01;

import No07_案例.Bean.AdWindowCnt;
import No07_案例.Bean.AdsClickLog;
import akka.japi.tuple.Tuple3;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * 每隔5s 1h内的 按照省份  统计广告点击次数
 * 一天内，如果对同一个广告点击超过一定限额（比如100次），应该把该用户加入黑名单并报警，此后其点击行为不应该再统计
 * 报警走侧输出流
 *
 *   (（天，用户，广告），cnt)
 *
 */
public class _11_监控_SlideWindowWordCount {
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


        KeyedStream<AdsClickLog, Tuple3<Long, Long, String>> keyedStream = adsClickDS.keyBy(new KeySelector<AdsClickLog, Tuple3<Long, Long, String>>() {
            @Override
            public Tuple3<Long, Long, String> getKey(AdsClickLog value) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String day = sdf.format(new Date(value.getTimestamp() * 1000));
                return new Tuple3<>(value.getUserId(), value.getAdId(), day);
            }
        });

        SingleOutputStreamOperator<String> res = keyedStream.process(new KeyedProcessFunction<Tuple3<Long, Long, String>, AdsClickLog, String>() {
            ValueState<Integer> count;

            @Override
            public void open(Configuration parameters) throws Exception {
                count = getRuntimeContext().getState(
                        new ValueStateDescriptor<Integer>(
                                "cnt",
                                Integer.class
                                ,0
                        )
                );
            }

            @Override
            public void processElement(AdsClickLog value, Context ctx, Collector<String> out) throws Exception {
                count.update(count.value() + 1);
                if (count.value() > 100) {
                    String warn = "警告： " + ctx.getCurrentKey() + "已经点击了" + count.value() + "次了！";
                    ctx.output(new OutputTag<String>("warn"){}, warn);
                } else {

                    out.collect(ctx.getCurrentKey() + "点击了" + count.value());
                }

            }
        });


        res.print();
        res.getSideOutput(new OutputTag<String>("warn"){}).print("warning");
        env.execute();
    }

}
