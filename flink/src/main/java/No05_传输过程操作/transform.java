package No05_传输过程操作;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class transform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop102",9999);


        source.keyBy(new KeySelector<String,String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).print("keyBy");
        //keyBy:2> hello
        //keyBy:2> hello
        //keyBy:2> hello
        //keyBy:2> hello
        //keyBy:2> hello


        source.shuffle().print("shuffle");//随机

        source.rebalance().print("rebalance");// 轮询 1 2 3 4 5 6
        source.rescale().print("rescale");// 轮询 1 2 3 4 5 6


        SingleOutputStreamOperator<String> map = source.map(x -> x).setParallelism(2);
         map.print("Map").setParallelism(2);


        map.rebalance().print();  // 2到4
        map.rescale().print();





        source.global().print("Global");//全是1  分区号是0




        //source.forward().print("forward");  //报错  上下游算子必须相同并行度

        source.broadcast().print("Broadcast");
        //广播：一条数据 打印6条
        //Broadcast:1> hello
        //Broadcast:3> hello
        //Broadcast:2> hello
        //Broadcast:4> hello
        //Broadcast:6> hello
        //Broadcast:5> hello



        env.execute();





    }
}
