package No11_FlinkSQL;


import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL02_StreamToTable_Agg {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        Table selectTable = sebsorTable.where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("sum_vc"))
                .select($("id"), $("sum_vc"));


        //老版本写法，将函数用.
        Table t2 = sebsorTable.groupBy("id")
                .select("id,vc.sum");

        //todo 聚合操作不能用追加流
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(selectTable, Row.class);

        tuple2DataStream.print();

        env.execute();


    }
    //ws_001,1577844001,45
    //(true,ws_001,45)

    //ws_001,1577844001,45
    //(false,ws_001,45)
    //(true,ws_001,90)

    //ws_001,1577844001,45
    //(false,ws_001,90)
    //(true,ws_001,135)
}
