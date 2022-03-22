
package No09_state._01_状态的使用;

import Bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class _02_ValueState_Time {
    //需求：监控温度传感器的温度值，如果温度值在10秒钟之内(processing time)没有下降，则报警。
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);


        KeyedStream<SensorReading,Tuple> sensorReadingTupleKeyedStream = source.map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String s) throws Exception {
                        String[] fields = s.split(",");
                        String name = fields[0];
                        Long ts = Long.parseLong(fields[1]);
                        double temp = Double.parseDouble(fields[2]);

                        return new SensorReading(name, ts, temp);
                    }
                })
                .keyBy("name");


        //定时器 必须要用processFunc
        SingleOutputStreamOperator<String> res = sensorReadingTupleKeyedStream.process(new TempIncreaseWarning(10));

        res.print();

        env.execute();

    }

    public static class TempIncreaseWarning extends KeyedProcessFunction<Tuple, SensorReading, String>{
        private int interval;

        public TempIncreaseWarning() {
        }

        public TempIncreaseWarning(int interval) {
            this.interval = interval;
        }

        // 声明状态，保存上次的温度值、当前定时器时间戳
        private ValueState<Double> lastTempState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 取出状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

           // 更新温度状态
            lastTempState.update(value.getTemp());
            //温度上升
            if( value.getTemp() > lastTemp && timerTs == null ){
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            }
            else if( value.getTemp() < lastTemp && timerTs != null){
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect( "传感器" + ctx.getCurrentKey() + "的温度连续" + interval + "秒上升" );
            // 清空timer状态
            timerTsState.clear();
        }
    }

}
