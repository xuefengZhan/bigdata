package No09_state._01_状态的使用;

import Bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _09_operator_ListState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> map = source.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] datas = s.split(",");
                return new SensorReading(datas[0], Long.valueOf(datas[1]), Double.valueOf(datas[2]));
            }
        });


    }

    // 算子状态 - listState  统计元素个数
    public static class myMapFunc extends RichMapFunction<SensorReading, Integer> implements CheckpointedFunction{

        private ListState<Integer> listState;


        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            return null;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

        }
    }

}
