package No11_FlinkSQL.TableAPI;


import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_StreamToTable_Test {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> inputDataStream = env.socketTextStream("hadoop102", 9999);
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

        //todo 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 流转化为表
        Table sebsorTable = tableEnv.fromDataStream( waterSensorDS);

        //todo TableAPI 在流上进行查询
        Table res = sebsorTable.where($("id").isEqual("ws_001"))
                .select($("id"),$("ts"), $("vc"));

        Table res2 = sebsorTable.where("id = 'ws_001'")
                .select("id,ts,vc"); //老版本写法


        //todo 将结果表转换成流打印  转换成一个类型进行打印 ROW是通用的
        DataStream<Row> rowDataStream = tableEnv.toAppendStream(res, Row.class);

        rowDataStream.print();

        env.execute();


    }
    //ws_001,1577844001,45
    //ws_002,1577844015,43
}
