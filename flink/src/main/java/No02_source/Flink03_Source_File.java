package No02_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_File {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从集合中读取数据
        DataStreamSource<String> fileDS = env.readTextFile("input/sensor-data.log");

        // 2.打印
        fileDS.print();

        // 3.执行
        env.execute();
    }
}
