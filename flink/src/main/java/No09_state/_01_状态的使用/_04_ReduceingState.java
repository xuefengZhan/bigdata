
package No09_state._01_状态的使用;

import Bean.SensorReading;
import akka.stream.impl.ReducerState;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _04_ReduceingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        KeyedStream<SensorReading, Tuple> keyedStream = source.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] datas = s.split(",");
                return new SensorReading(datas[0], Long.valueOf(datas[1]), Double.valueOf(datas[2]));
            }
        }).keyBy("name");


        keyedStream.map(new RichMapFunction<SensorReading, Tuple2<String, Double>>() {

            //todo reduceingstate的特点是：只有一个泛型，输入和输出是一样的
            ReducingState<SensorReading> reducerState;

            @Override
            public void open(Configuration parameters) throws Exception {
                reducerState = getRuntimeContext().
                        getReducingState(new ReducingStateDescriptor<SensorReading>(
                                "reduce-state",
                                new ReduceFunction<SensorReading>() {
                                    @Override
                                    public SensorReading reduce(SensorReading t1, SensorReading t2) throws Exception {
                                        return new SensorReading(t1.getName(), t1.getTs(), t1.getTemp() + t2.getTemp());
                                    }
                                }, SensorReading.class
                        ));
            }

            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {

                //聚合
                reducerState.add(sensorReading);

                SensorReading res = reducerState.get();


                return new Tuple2<>(res.getName(), res.getTemp());
            }
        }).print();

        env.execute();
    }
}
