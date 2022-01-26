package No11_FlinkSQL.SQL;

import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 *  写到文件系统只能用追加流，因此sql不能有聚合操作
 *  文件系统无法撤回某一条数据
 */
public class FlinkSQL10_SQL_Agg {
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

        //注册临时表
        // 方式1 ： 流上建立
         tableEnv.createTemporaryView("sensor",waterSensorDS);

         //方式2：  table上建立
//        Table table = tableEnv.fromDataStream(waterSensorDS);
//        tableEnv.createTemporaryView("sensor",table);

        //todo 2. SQL查询
        Table result = tableEnv.sqlQuery("select id,count(ts),sum(vc) from sensor where id = 'ws_001' group by id");

        tableEnv.toRetractStream(result, Row.class).print();
        env.execute();




    }
}
