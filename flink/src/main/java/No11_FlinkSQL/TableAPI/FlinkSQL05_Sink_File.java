package No11_FlinkSQL.TableAPI;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
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
public class FlinkSQL05_Sink_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //todo 1. source
        //通过connector 创建临时表
        tableEnv.connect(new FileSystem().path("E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort.txt"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("vc",DataTypes.INT()))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .createTemporaryTable("sensor");

        //创建source table
        Table sensor = tableEnv.from("sensor");

        //todo 2. 查询
        Table resultTable = sensor.where($("id").isEqual("ws_001"))
                .select($("id"), $("ts"),$("vc") );


        //todo 3. 写出
        //创建写出临时表
        tableEnv.executeSql("create table sensorOut(" +
                "id String," +
                "ts bigint," +
                "vc int)" +
                "with(" +
                "'connector' = 'filesystem'," +
                "'path' = 'E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort2.txt," +
                "'format' = 'Csv')");
//        tableEnv.connect(new FileSystem().path("E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort2.txt"))
//                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("ts",DataTypes.BIGINT())
//                        .field("vc",DataTypes.INT()))
//                .createTemporaryTable("sensorOut");
        //结果写到写出临时表
        resultTable.executeInsert("sensorOut");

        env.execute();


    }
}
