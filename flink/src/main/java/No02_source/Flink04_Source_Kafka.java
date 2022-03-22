package No02_source;

import Bean.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

public class Flink04_Source_Kafka {



    /**
     * 从kafka中读取数据
     */
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从kafka中读取
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.4.86:9092");
        properties.setProperty("group.id", "flink_release");

        FlinkKafkaConsumer010<String> kafkaConsumer = new FlinkKafkaConsumer010<String>("yjp_trace_v4_stream", new SimpleStringSchema(), properties);
        // 指定起始位置
        //kafkaConsumer.setStartFromEarliest();
        kafkaConsumer.setStartFromLatest();

        //kafkaConsumer.setStartFromGroupOffsets();
        //kafkaConsumer.setStartFromTimestamp(); // kafka 0.10版本后 Segment文件中除了 index 和 log 还有timestampIndex文件

        DataStreamSource<String> kafkaDS = env.addSource(kafkaConsumer);

        kafkaDS.print("kafka source");

        env.execute();
    }

}
