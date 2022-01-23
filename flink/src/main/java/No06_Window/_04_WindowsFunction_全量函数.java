package No06_Window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class _04_WindowsFunction_全量函数 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> kStream = map.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> ttwindow = kStream.timeWindow(Time.seconds(5));
        //todo  用全量函数做窗口的wordCount 全量函数有两种：apply和ProcessWindowFunction
        // 此处展示apply()的使用
        // apply的参数是windowFunction,windowFunction是一个接口，继承了Function和序列化
        SingleOutputStreamOperator<Tuple2<String, Integer>> apply = ttwindow.apply(new myWindowFunction());

        apply.print();

        env.execute();

    }

    //todo 做wordCount  windowFunction需要四个泛型
//        * @param <IN> The type of the input value.
//        * @param <OUT> The type of the output value.
//        * @param <KEY> The type of the key.（就是keydStream的key类型）
//        * @param <W> The type of {@code Window} that this window function can be applied on.
//
    public static class myWindowFunction
            implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>,
            Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            //TODO 实现apply方法，四个参数：
            //  第一个就是KeyedStream的key
            //  第二个是window  可以获取窗口的信息，比如窗口的起始和结束时间
            //  第三个是窗口所有数据组成的迭代器
            //  第四个是Collector，用于写出数据的

            //todo 1.获取key   改成String类型
            String key = tuple.getField(0);
            // todo 2.全量函数会将窗口key相同的数据装到一个迭代器中
            //  遍历迭代器处理key相同的数据集
            Iterator<Tuple2<String, Integer>> inputIter = input.iterator();
            int count = 0;
            while (inputIter.hasNext()) {
                count += inputIter.next().f1;
            }
            // todo 3.处理完毕 用collector进行输出  和 flatMap一样
            out.collect(new Tuple2<String, Integer>(key, count));
        }

    }
}
