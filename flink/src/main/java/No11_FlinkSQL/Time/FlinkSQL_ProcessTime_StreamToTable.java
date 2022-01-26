package No11_FlinkSQL.Time;


import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.text.SimpleDateFormat;

import static org.apache.flink.table.api.Expressions.$;

/**
 *  流转换表的时候引入处理时间
 */
public class FlinkSQL_ProcessTime_StreamToTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        DataStreamSource<String> sourceDS = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort.txt");
        SingleOutputStreamOperator<WaterSensor> sensorDS = sourceDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        Table table = tableEnv.fromDataStream(sensorDS,$("id"),$("ts"),$("vc"),$("pt").proctime());

        table.printSchema();
        //root
        // |-- id: STRING
        // |-- ts: BIGINT
        // |-- vc: INT
        // |-- pt: TIMESTAMP(3) *PROCTIME*

        DataStream<Row> res = tableEnv.toAppendStream(table, Row.class);

        res.print();

        env.execute();
    }
}
