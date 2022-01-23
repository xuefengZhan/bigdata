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
import org.apache.flink.util.OutputTag;

public class Watermark_lateness {
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
                kb.timeWindow(Time.seconds(5))
                        .allowedLateness(Time.seconds(2))
                        .sideOutputLateData(new OutputTag<Tuple2<String,Integer>>("sideOutput"){});

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(1);


        result.print("result");

        result.getSideOutput(new OutputTag<Tuple2<String,Integer>>("sideOutput"){}).print("sideOutPut");


        env.execute();


        //计算 窗口关闭  输出数据  都可以分别控制
        //增量聚合：来一条数据 只做计算，窗口关闭同时输出数据
        //          输出时间点                                        输出内容
        //   watermark >= end_time                          做一次[start_time,end_time) 统一的计算结果
        //end_time <= waterMark < ent_time + t_late         [start_time,end_time + ]


        //sensor_1,1547718199,35.8
        //sensor_1,1547718202,56   wm=200  触发窗口[195,200)做一次计算                 result:10> (sensor_1,1)
        //sensor_1,1547718198,23   只要wm没有到200+2,只要数据落在[195,200)的都会出发计算      result:10> (sensor_1,2)
        //sensor_1,1547718195,56                                                    result:10> (sensor_1,3)
        //当wm > 200 + 2 的时候，此时[195,200) 真正关闭，再来迟到的数据进入测输出流

    }
}
