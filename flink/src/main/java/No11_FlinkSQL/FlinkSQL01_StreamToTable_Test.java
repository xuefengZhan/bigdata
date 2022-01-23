package No11_FlinkSQL;

import No01_WordCount.MyFlatMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkSQL01_StreamToTable_Test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");


        DataStream<String> inputDataStream = env.socketTextStream("hadoop102", 9999);

        //todo 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);




    }
}
