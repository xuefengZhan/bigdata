
package No09_state.键控状态;



import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;

public class _01_wordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\hello.txt");
        SingleOutputStreamOperator<Tuple2<String, Integer>> flat = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> key = flat.keyBy(0);


        //todo 使用状态编程实现wordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = key.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            //todo 状态编程 step1.  RichFunction 或者 ProcessFunction 实现类

            //todo 状态编程 step2.  声明状态变量属性
            // 状态变量只能声明 不能初始化，因为类加载的时候还没有运行时上下文
            // 也不能在处理方法中初始化，否则每一个元素有一个状态了，目的是每个key的数据共享一个状态
            // 在open方法做初始化，这个状态是当前key的stream所共享的
            // 默认值为null 所以一般都需要初始化才能做后续计算

            //todo 在scala中，添加lazy 懒加载即可
            ValueState<Integer> count;
            @Override
            public void open(Configuration parameters) throws Exception {
                //todo 状态编程step3 .open方法在处理从运行时上下文获取上一条数据处理后的状态
                count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                        "word-count", Integer.class, 0
                ));

                //TODO 状态初始化的错误写法写法:
                //   count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(
                //                   "word-count", Integer.class));
                //   count.update(0);
                // 报错如下：
                // Caused by: java.lang.NullPointerException: No key set. This method should not be called outside of a keyed context.
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                count.update(count.value() + value.f1);
                return new Tuple2<>(value.f0, count.value());
            }
        });


        map.print("wordcount");
        env.execute();
    }
}


