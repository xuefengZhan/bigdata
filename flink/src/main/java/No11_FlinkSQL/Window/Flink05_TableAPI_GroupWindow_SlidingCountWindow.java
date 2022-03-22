package No11_FlinkSQL.Window;

import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rowInterval;

public class Flink05_TableAPI_GroupWindow_SlidingCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

       // DataStreamSource<String> sourceDS = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\sensort.txt");
        DataStreamSource<String> sourceDS = env.socketTextStream("hadoop102", 9999);

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

        //滚动计数窗口 over.every.on.as   计数窗口也要时间
        // 第一个窗口集满5个才打印，后面每2个打印一次  和API方式不同，API是两个一打
        Table res = table.window(Slide.over(rowInterval(5L)).every(rowInterval(2L)).on($("pt")).as("cw"))
                .groupBy($("id"), $("cw"))
                .select($("id"), $("id").count());


        res.printSchema();
        //root
        // |-- id: STRING
        // |-- EXPR$0: BIGINT

        //结果表转为流输出
        //todo 说明：窗口聚合由于只输出一次，因此可以用追加流
        DataStream<Row> tuple = tableEnv.toAppendStream(res, Row.class);

        tuple.print();

        env.execute();

        //ws_001,1547718200,36
        //ws_001,1547718200,36
        //ws_001,1547718200,36
        //ws_001,1547718200,36
        //ws_001,1547718200,36
        //输出->ws_001,5

        //ws_001,1547718200,36
        //ws_001,1547718200,36
        //ws_001,5

        //ws_001,1547718200,36
        //ws_001,1547718200,36
        //ws_001,5
    }
}
