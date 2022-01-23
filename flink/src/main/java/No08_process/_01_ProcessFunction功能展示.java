package No08_process;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class _01_ProcessFunction功能展示 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
    }
    public static class MyProcessFunc extends ProcessFunction<String,String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            //TODO 功能1 获取运行时上下文  用于状态编程   (此方法继承自RichFunction)
            RuntimeContext runtimeContext = getRuntimeContext();
            // 可以获取状态
            //runtimeContext.getState();
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        //TODO 针对DataStream中的每一个Element调用此方法，返回值是void，可以自己决定是否有输出
        // 如果要输出，用collector输出
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            // todo 功能3-out往主流输出
            out.collect(" ");

            //todo 功能2-ctx 获取处理时间、注册处理时间定时器，删除处理时间定时器
            ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(1L);
            ctx.timerService().deleteProcessingTimeTimer(1L);
            //todo 功能2 获取事件时间、注册事件时间定时器，删除事件时间定时器
            ctx.timerService().currentWatermark();
            ctx.timerService().registerEventTimeTimer(1L);
            ctx.timerService().deleteEventTimeTimer(1L);

            //todo 功能4-ctx: 往侧输出流写出  ctx.output
            //ctx.output(new OutputTag<String>("outPutTag"){},value);


        }

        @Override
        //todo 功能5： 指定定时器触发时任务执行
        // ctx 还可以再次定闹钟
        //out 往主流输出
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }
    }

}
