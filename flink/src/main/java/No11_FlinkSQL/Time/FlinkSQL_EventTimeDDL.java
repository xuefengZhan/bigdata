package No11_FlinkSQL.Time;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL_EventTimeDDL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(" CREATE TABLE source_sensor (\n" +
                "                id String,\n" +
                "                ts bigint,\n" +
                "                vc int,\n" +
                "                rt as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss'))," +
                "                WATERMARK FOR rt AS rt - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "'connector' = 'filesystem'," +
                "'path' = 'E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort.txt'," +
                "'format' = 'csv')" );

        Table source_sensor = tableEnv.from("source_sensor");
        source_sensor.printSchema();

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(source_sensor, Row.class);
        rowDataStream.print();

        env.execute();


    }
}
