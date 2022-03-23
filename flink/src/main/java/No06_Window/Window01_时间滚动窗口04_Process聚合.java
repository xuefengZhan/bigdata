package No06_Window;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

public class Window01_时间滚动窗口04_Process聚合 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> DS1 = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = s.split(" ");
                for (String field : fields) {
                    collector.collect(new Tuple2(field, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = DS1.keyBy(0);

        //todo 开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //todo process方法做 全量窗口计算 process中处理的数据是窗口全量的数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = window.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

                //1. ProcessWindowFunction中 context 可以获取的信息：
                context.window().getEnd();
                context.window().getStart();
                context.currentWatermark();
                context.currentProcessingTime(); //当前窗口的运行时间

                //2. RuntimeContext
                getRuntimeContext().getIndexOfThisSubtask();


                ArrayList<Tuple2<String, Integer>> arrays = Lists.newArrayList(elements.iterator());
                int cnt = arrays.size();
                long start = context.window().getStart();
                out.collect(new Tuple2<>(new Timestamp(start) + (String) tuple.getField(0), cnt));
            }
        });

        //全量窗口函数 可以获取窗口信息

        res.print();

        env.execute();

    }
}
