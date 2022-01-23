package No08_process;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class _04_侧输出流 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        KeyedStream<String, String> keyedStream = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        });

        //todo 将温度小于30度的输出到侧输出流，大于30度的输出的主流
        SingleOutputStreamOperator<String> result = keyedStream.process(new mySplit());

        result.print("high");
        result.getSideOutput(new OutputTag<Tuple2<String,Double>>("<30"){}).print("sideOut");
        env.execute();

    }
    public static class mySplit extends KeyedProcessFunction<String,String,String> {

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            //todo 获取温度
            String[] fields = value.split(",");
            double temp = Double.parseDouble(fields[2]);

            if(temp >= 30){
                out.collect(value);
            }else{
                //测输出流的数据类型没有限定，输出时定义
                ctx.output(new OutputTag<Tuple2<String,Double>>("<30"){},new Tuple2<String,Double>(fields[0],temp));
            }
            //todo 说明：官方之所以推荐用这种方式对流进行split，是因为侧输出流可以和主流的数据类型不同

        }
    }

}
