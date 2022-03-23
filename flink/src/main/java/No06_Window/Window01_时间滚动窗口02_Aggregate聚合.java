package No06_Window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Window01_时间滚动窗口02_Aggregate聚合 {
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

        KeyedStream<Tuple2<String, Integer>,String> keyedStream = DS1.keyBy(t->t.f0);

        //todo 开窗
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        //todo 增量计算 +  窗口
        // 原理：先做增量计算，然后将整个窗口的计算结果作为输入，放进WindowFunction，
        // 从而实现既能够增量也能够获取窗口信息
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = window.aggregate(new MyAggFunc(), new MyWindowFunc());


        //todo 输出特点
        //1.没有数据过来就不输出
        //2.每一次只输出该窗口内的计算结果

        res.print();

        env.execute();

    }

    public static class MyAggFunc implements AggregateFunction<Tuple2<String, Integer>,Integer,Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> stringIntegerTuple2, Integer integer) {
            return integer + 1;
        }

        @Override
        public Integer getResult(Integer integer) {
            return integer;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    //Type parameters:
    //<IN> –     aggregateFunc的输出
    //<OUT> –    输出类型
    //<KEY> – The type of the key.
    //<W> – The type of Window that this window function can be applied on.
    public static class MyWindowFunc implements WindowFunction<Integer,Tuple2<String,Integer>,String,TimeWindow>{

        //迭代器中只有一条数据
        @Override
        public void apply(String s, TimeWindow window, Iterable<Integer> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer cnt = input.iterator().next();
            out.collect(new Tuple2<>(new Timestamp(window.getStart()) + s,cnt));
        }
    }
}
