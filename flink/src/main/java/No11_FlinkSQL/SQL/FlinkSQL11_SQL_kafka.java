package No11_FlinkSQL.SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL11_SQL_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TableResult sourceTable = tableEnv.executeSql("CREATE TABLE source_sensor (" +
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

        tableEnv.executeSql("CREATE TABLE sink_sensor (" +
                "  `id` STRING," +
                "  `ts` BIGINT," +
                "  `vc` INT," +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'topic_sink'," +
                "  'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'csv'" +
                ")");


        tableEnv.executeSql("insert into sink_sensor select * from source_sensor where id = 'ws_001' ");
        env.execute();
    }
}
