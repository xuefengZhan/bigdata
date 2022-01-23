package No08_process;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class _03_定时器 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);


        SingleOutputStreamOperator<String> res = source.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        }).process(new MyOnTimerProcessFunc());

        //定时器功能只能用于KeyedStream

        res.print();

        env.execute();
    }


    //todo 实现处理完当前数据两秒后输出一条数据
    public static class MyOnTimerProcessFunc extends KeyedProcessFunction<String,String,String>{

        @Override
        public void processElement(String value,Context ctx, Collector<String> out) throws Exception {
            out.collect(value);

            //注册两秒后的闹钟
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 2000L);
        }


        //闹钟响了 定时任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            System.out.println("定时器触发了");
        }
    }


}
