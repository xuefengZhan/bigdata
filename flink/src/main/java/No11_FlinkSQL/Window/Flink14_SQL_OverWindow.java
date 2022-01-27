package No11_FlinkSQL.Window;

import Bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Flink14_SQL_OverWindow {

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


        Table result = tableEnv.sqlQuery("select " +
                "id, " +
                "sum(vc) over(partition by id order by pt) sum_vc, " +
                "count(id) over(partition by id order by pt) count_id " +
                "from " + table);

       // result.execute().print();


        Table result2 = tableEnv.sqlQuery("select " +
                "id, " +
                "sum(vc) over w as sum_vc, " +
                "count(id) over w as count_id " +
                "from " + table +
                " window w as (partition by id order by pt) ");
        //window w as  不要加 over


        //flinkSQL 中一个SQL over()窗口必须一致
        //HiveSQL 中可以不一样

        result2.execute().print();

        env.execute();
    }
}
