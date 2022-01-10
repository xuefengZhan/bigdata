package No01_WordCount;

import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 有界流处理
 *
 * 流处理用的是DataStream API
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {

        //todo 1.创建流的环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 2.创建source
        String inputPath = "D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> source = env.readTextFile(inputPath);

        //todo 3.基于DataStream 转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = source.flatMap(new MyFlatMapper());


        //todo 4.分组   流处理用的是keyBy  批处理是groupBy
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = wordToOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = tuple2TupleKeyedStream.sum(1);

        res.print();


        //todo 5.流处理要启动应用
        env.execute();


        //9> (world,1)
        //1> (scala,1)
        //13> (flink,1)
        //5> (hello,1)
        //5> (hello,2)
        //5> (hello,3)
        //前缀是线程编号
    }

}
