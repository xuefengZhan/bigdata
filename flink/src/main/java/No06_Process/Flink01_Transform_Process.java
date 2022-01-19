package No06_Process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_Transform_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> source = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\hello.txt");


        //todo 1.process实现flatMap
        KeyedStream<String, String> keyedStream = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                String[] fields = value.split(" ");
                for (String field : fields) {
                    out.collect(field);
                }
            }
        }).keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        //todo 2.process 实现sum()
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyedStream.process(new KeyedProcessFunction<String, String, Tuple2<String, Integer>>() {

            ValueState<Integer> cnt;

            @Override
            public void open(Configuration parameters) throws Exception {
                cnt = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("cnt", Integer.class, 0));
            }

            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer count = cnt.value();
                cnt.update(count + 1);

                out.collect(new Tuple2<>(value, count + 1));
            }
        });

        res.print();
        env.execute();
    }
}
