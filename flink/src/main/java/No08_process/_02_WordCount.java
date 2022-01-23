package No08_process;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class _02_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //todo 用Process Function 实现flatMapFunc  MapFunc,sum
        SingleOutputStreamOperator<String> flat = source.process(new MyFlatMapFun());
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = flat.process(new MyMapFun());
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = map.keyBy(0).process(new MySumFun());
        result.print();
        env.execute();



        //processFunction 用于窗口中，是全量窗口函数


    }


    //todo 1.做flatMap的功能
    public static class MyFlatMapFun extends ProcessFunction<String, String> {

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] fields = value.split(" ");
            for (String field : fields) {
                out.collect(field);
            }
        }
    }

    //todo 2.做map功能
    public static class MyMapFun extends ProcessFunction<String, Tuple2<String, Integer>> {
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<String, Integer>(value, 1));
        }
    }

    //todo 3.做sum功能
    // * @param <K> Type of the key.
    // * @param <I> Type of the input elements.
    // * @param <O> Type of the output elements.
    public static class MySumFun extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        //todo 在这里定义并行度内共享，并非一个key共享   一个分区内的所有数据都会进行累加
        //这里用一个变量，程序挂了就没了
        private int count = 0;


        @Override
        public void open(Configuration parameters) throws Exception {
            //getRuntimeContext().getState()
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            count++;
            out.collect(new Tuple2<String, Integer>(value.f0, count));
        }
    }
}