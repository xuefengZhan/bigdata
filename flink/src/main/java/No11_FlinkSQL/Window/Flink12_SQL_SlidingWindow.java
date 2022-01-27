package No11_FlinkSQL.Window;

import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class Flink12_SQL_SlidingWindow {

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


        //滑动窗口用hop 并且前面是slid,后面是size
        Table result = tableEnv.sqlQuery("" +
                "select id," +
                "       count(id)," +
                "       hop_start(pt,interval '2' second , interval '6' second)" +
                "from " + table +
                " group by id," +
                "       hop(pt,interval '2' second , interval '6' second)");


        //tableEnv.toAppendStream(result, Row.class).print();

        result.execute().print();

        env.execute();
    }
}
