package No11_FlinkSQL.Window;

import Bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink10_TableAPI_OverWindow_Unbounded {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        DataStreamSource<String> sourceDS = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort.txt");

        SingleOutputStreamOperator<WaterSensor> sensorDS = sourceDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String s) throws Exception {
                String[] split = s.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        Table table = tableEnv.fromDataStream(sensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());


        //preceding(UNBOUNDED_ROW)
        Table result = table.window(Over.partitionBy("id").orderBy($("pt")).as("ow"))
                .select($("id"),
                        $("vc").sum().over($("ow")),
                        $("id").count().over($("ow")));


        tableEnv.toAppendStream(result, Row.class).print();

        //ws_001,1577844001,45
        //ws_001,1577844001,45
        //ws_002,1577844015,43
        //ws_002,1577844015,43
        //ws_001,45,1
        //ws_001,90,2
        //ws_002,43,1
        //ws_002,86,2

        env.execute();
    }
}
