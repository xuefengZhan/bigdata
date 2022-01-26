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

public class FlinkSQL03_Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        /**
         * connector适用于数据是结构化数据，如果是非结构化数据需要先用DataStream处理一下，再转成table
         */
        //通过connector 创建临时表
        tableEnv.connect(new FileSystem().path("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\sensort.txt"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("vc",DataTypes.INT()))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .createTemporaryTable("sensor");

        Table sensor = tableEnv.from("sensor");

        //查询
        Table resultTable = sensor.groupBy($("id"))
                .select($("id"), $("id").count().as("ct"));

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(resultTable, Row.class);

        tuple2DataStream.print();

        env.execute();


    }
}
