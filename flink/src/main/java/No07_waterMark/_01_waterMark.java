package No07_waterMark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class _01_waterMark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 第一步：引入事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //todo 第二步：提取流中的数据的时间字段作为事件时间
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = source.assignTimestampsAndWatermarks(
                //周期性waterMark  有界无序   参数是最大乱序时间
                new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
                    @Override
                    //todo 抽取事件中的时间戳，element是事件
                    public long extractTimestamp(String element) {
                        String[] fields = element.split(",");
                        return Long.parseLong(fields[1]) * 1000L;
                    }
                }
        );

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = stringSingleOutputStreamOperator.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new Tuple2<String, Integer>(fields[0], 1);
                    }
                }
        );

        KeyedStream<Tuple2<String, Integer>, Tuple> kb =map.keyBy(0);
        //todo 创建窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window =
                kb.timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(1);
        result.print();
        env.execute();


        //sensor_1,1547718199,35.8    wm = 197
        //sensor_1,1547718199,35.8    wm = 197
        //sensor_1,1547718200,35.8    wm = 198
        //sensor_1,1547718203,35.8    wm = 201   =>    10> (sensor_1,2)

    }

}
