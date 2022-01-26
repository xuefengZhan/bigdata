package No11_FlinkSQL.SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL12_SQL_Kafka2Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE source_sensor (" +
                "  `id` STRING," +
                "  `ts` BIGINT," +
                "  `vc` INT," +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'topic_source'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")");


        tableEnv.executeSql("create table sink_sensor(" +
                "  `id` STRING," +
                "  `ts` BIGINT," +
                "  `vc` INT," +
                ") WITH (" +
                "     'connector' = 'jdbc'," +
                "      'url' = 'jdbc:mysql://hadoop102:3306/flink'," +
                "      'table-name' = 'sensor'" +
                "      'username' = 'root'" +
                "      'password' = '123456')"
        );


        Table source_sensor = tableEnv.from("source_sensor");

        source_sensor.executeInsert("sink_sensor");

        env.execute();

    }


}
