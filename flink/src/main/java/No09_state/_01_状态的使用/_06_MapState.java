package No09_state._01_状态的使用;

import Bean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class _06_MapState {
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


        //todo 重复的不输出
        keyedStream.process(new KeyedProcessFunction<Tuple, SensorReading,SensorReading>() {
            private MapState<String,String> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext()
                        .getMapState(new MapStateDescriptor< String, String>("map-state",
                                String.class,String.class));
            }

            @Override
            public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                //取出
                String name = value.getName();
                String v = mapState.get(name);
                if(v == null){
                    mapState.put(name,"");
                    out.collect(value);
                }
            }
        }).print();
        env.execute();
    }
}
