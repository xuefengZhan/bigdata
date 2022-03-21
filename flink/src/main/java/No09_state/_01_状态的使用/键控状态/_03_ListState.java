
package No09_state._01_状态的使用.键控状态;

import Bean.SensorReading;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 *  每个传感器输出最高的三个温度值
 *  
 */
public class _03_ListState {
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

        //todo 使用ListState实现每个传感器最高的三个水位线
        SingleOutputStreamOperator<List<SensorReading>> res = keyedStream.map(new RichMapFunction<SensorReading, List<SensorReading>>() {

            //1.定义状态
            private ListState<SensorReading> top3;

            @Override
            public void open(Configuration parameters) throws Exception {
                top3 = getRuntimeContext().getListState(new ListStateDescriptor<SensorReading>("list-state", SensorReading.class));
            }

            @Override
            public List<SensorReading> map(SensorReading sensorReading) throws Exception {

                top3.add(sensorReading);

                ArrayList<SensorReading> sensorReadings = Lists.newArrayList(top3.get().iterator());

                sensorReadings.sort((x, y) -> (int) (x.getTemp() - y.getTemp()));

                //判断list是否超过三个，如果超过删除最后一条
                if (sensorReadings.size() > 3) {
                    sensorReadings.remove(3);
                }

                top3.update(sensorReadings);

                return sensorReadings;
            }
        });

        res.print();

        env.execute();
    }
}
