package No02_source;

import Bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从集合中读取数据
        DataStreamSource<WaterSensor> collectionDS = env.fromCollection(
                Arrays.asList(
                        new WaterSensor("ws_001", 1577844001L, 45),
                        new WaterSensor("ws_002", 1577844015L, 43),
                        new WaterSensor("ws_003", 1577844020L, 42)
                )
        );

        // 2.打印
        collectionDS.print();

        // 3.执行
        env.execute();
    }
}
