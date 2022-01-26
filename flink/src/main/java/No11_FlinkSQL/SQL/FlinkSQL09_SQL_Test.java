package No11_FlinkSQL.SQL;

import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
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

import static org.apache.flink.table.api.Expressions.$;

/**
 *  写到文件系统只能用追加流，因此sql不能有聚合操作
 *  文件系统无法撤回某一条数据
 */
public class FlinkSQL09_SQL_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 1.source
        //DataStream<String> inputDataStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> inputDataStream= env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort.txt");
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = inputDataStream.map(new MapFunction<String, WaterSensor>() {

            @Override
            public WaterSensor map(String s) throws Exception {
                String[] fields = s.split(",");
                return new WaterSensor(
                        fields[0],
                        Long.parseLong(fields[1]),
                        Integer.parseInt(fields[2])
                );
            }
        });

        //如果datastream中数据类型是Tuple,可以用这种方式定义字段名
        //Table table = tableEnv.fromDataStream(waterSensorDS, $("id"), $("ts"), $("vc"));

        Table table = tableEnv.fromDataStream(waterSensorDS);


        //todo 2. SQL查询
        Table result = tableEnv.sqlQuery("select id,ts,vc from " + table + " where id = 'ws_001'");

        tableEnv.toAppendStream(result, Row.class).print();
        env.execute();


    }
}
