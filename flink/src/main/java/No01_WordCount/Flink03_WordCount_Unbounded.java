package No01_WordCount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 无界流
 */
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");


        DataStream<String> inputDataStream = env.socketTextStream("hadoop102", 9999);

        DataStream<Tuple2<String, Integer>> wordCountDataStream = inputDataStream
                .flatMap( new  MyFlatMapper())
                .keyBy(0)
                .sum(1);
        wordCountDataStream.print().setParallelism(1);

        env.execute();

        //[atguigu@hadoop102 ~]$ nc -lk 9999
        //hello word
        //hello fantasy
        //hello flink

        //(hello,1)
        //(word,1)
        //(hello,2)
        //(fantasy,1)
        //(hello,3)
        //(flink,1)


    }
}
