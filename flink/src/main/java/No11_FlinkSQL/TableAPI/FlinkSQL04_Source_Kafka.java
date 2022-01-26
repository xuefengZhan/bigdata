package No11_FlinkSQL.TableAPI;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka().
                version("universal").
                topic("test").
                startFromLatest()
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG,"zxf")
        ).withSchema(new Schema()
                    .field("id", DataTypes.INT())
                    .field("name",DataTypes.STRING())
        ).withFormat(new Csv()) //字符串的格式
                .createTemporaryTable("sensor");


        Table sensor = tableEnv.from("sensor");

        //查询
        Table res = sensor.groupBy($("id"))
                .select($("id"), $("id").count());

        tableEnv.toRetractStream(res, Row.class).print();

        env.execute();
    }
}
