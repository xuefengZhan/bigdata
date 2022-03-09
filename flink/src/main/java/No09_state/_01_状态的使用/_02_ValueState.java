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
import org.apache.flink.util.Collector;

public class _02_ValueState {
    //实现这样一个需求：检测传感器的温度值，如果连续的两个温度差值超过10度，就输出报警。
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        KeyedStream<SensorReading, Tuple> sensorReadingTupleKeyedStream = source.map(new MapFunction<String, SensorReading>() {
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


        SingleOutputStreamOperator<String> res = sensorReadingTupleKeyedStream.flatMap(new myProcessFunc());

        res.print();

        env.execute();

    }

    //使用flatMap 是因为flatMap不强制输出； map必须要有返回值
    public static class myProcessFunc extends RichFlatMapFunction<SensorReading,String>{

        ValueState<Double> lastTemp ;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTemp =  getRuntimeContext().getState(new ValueStateDescriptor<Double>("state",Double.class));
        }

        @Override
        public void flatMap(SensorReading sensorReading, Collector<String> collector) throws Exception {
            Double last = lastTemp.value();
            double now = sensorReading.getTemp();

            if(last != null && Math.abs(last - sensorReading.getTemp()) >= 10){
                collector.collect(sensorReading.getName() + "上一次温度为" + last + "这一次温度为：" + sensorReading.getTemp());
            }
            lastTemp.update(now);
        }
    }
}
