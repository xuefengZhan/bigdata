package No11_FlinkSQL.案例;

import Bean.WaterSensor;
import No07_案例.Bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_TopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> sourceDS = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\Data\\UserBehavior.csv");

        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp();
            }
        });


        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = sourceDS.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] fields = value.split(",");
                long userId = Long.parseLong(fields[0]);
                long itemId = Long.parseLong(fields[1]);
                int categoryId = Integer.parseInt(fields[2]);
                String behavior = fields[3];
                long timestamp = Long.parseLong(fields[4]);

                return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
            }
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.getBehavior().equals("pv");
            }
        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = userBehaviorDS.assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        //TopN只能在Blink环境下执行

        Table sourceTable = tableEnv.fromDataStream(userBehaviorSingleOutputStreamOperator,
                $("userId"),
                $("itemId"),
                $("categoryId"),
                $("behavior"),
                $("timestamp"),
                $("ts").rowtime());

        Table res = tableEnv.sqlQuery("select " +
                "itemId, " +
                "count(itemId) as ct, " +
                "hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "from " + sourceTable +
                " where behavior = 'pv' " +
                "group by  " +
                "itemId, " +
                "hop(ts, interval '5' minute, interval '1' hour)");


        Table res2 = tableEnv.sqlQuery("select " +
                "itemId, " +
                "ct, " +
                "windowEnd, " +
                "row_number() over(partition by windowEnd order by ct)  as rk " +
                "from " + res);
//
//
//        Table res3 = tableEnv.sqlQuery("select * from " + res2 + " where rk <= 5");


        res2.printSchema();

        tableEnv.toAppendStream(res2, Row.class).print();

        env.execute();

    }
}
