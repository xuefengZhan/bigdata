package No11_FlinkSQL.Window;

import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class Flink01_TableAPI_GroupWindow_TumblingWindow {
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


        //todo 1. 引入时间语义
        Table table = tableEnv.fromDataStream(sensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        //todo 2.定义 滚动窗口： over.on.as
        Table res = table.window(Tumble.over(lit(5).seconds()).on($("pt")).as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count(),$("tw").proctime());


        //结果表转为流输出
        //todo 说明： 一个窗口只能输出一次，没有修改数据一说  因此可以用追加流
        DataStream<Row> tuple = tableEnv.toAppendStream(res, Row.class);

        tuple.print();

        env.execute();

    }
}
