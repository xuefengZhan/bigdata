package No11_FlinkSQL.Time;


import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 *  流转换表的时候引入处理时间
 */
public class FlinkSQL_ProcessTime_DDL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        DataStreamSource<String> sourceDS = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort.txt");

        tableEnv.executeSql(
                "create table sensor(" +
                          "id String," +
                          "ts bigint," +
                          "vc int," +
                          "pt_time as proctime())" +
                    "with(" +
                    "'connector' = 'filesystem'," +
                    "'path' = 'E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort2.txt'," +
                    "'format' = 'csv')" );


        TableResult tableResult = tableEnv.executeSql("select * from sensor where id = 'ws_001'");

        Table table = tableEnv.sqlQuery("select * from sensor where id = 'ws_001'");
        table.printSchema();
        //root
        // |-- id: STRING
        // |-- ts: BIGINT
        // |-- vc: INT
        // |-- pt: TIMESTAMP(3) *PROCTIME*
        tableResult.print();

        env.execute();
    }
}
