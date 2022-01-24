package No11_FlinkSQL;

import No01_WordCount.MyFlatMapper;
import org.apache.calcite.interpreter.Row;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkSQL01_StreamToTable_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");


        DataStream<String> inputDataStream = env.socketTextStream("hadoop102", 9999);

        //todo 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //todo 流转化为表
        Table sebsorTable = tableEnv.fromDataStream(inputDataStream);

        //todo TableAPI
//        Table res = sebsorTable.where($("id").isEqual("ws_001"))
//                .select($("id").$("ts"), $("vc"));

        Table res2 = sebsorTable.where("id = 'ws_001'")
                .select("id,ts,vc"); //老版本写法



        //todo 将结果表转换成流打印  转换成一个类型进行打印 ROW是通用的
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(res, Row.class);

//        rowDataStream.print();

        env.execute();


    }
}
