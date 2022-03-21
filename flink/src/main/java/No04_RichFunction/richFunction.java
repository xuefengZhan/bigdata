package No04_RichFunction;

import Bean.SensorReading;
import No03_transform.myFunction.MyMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Before;
import org.junit.Test;

public class richFunction {

    StreamExecutionEnvironment env;
    DataStream<String> source;

    @Before
    public void init() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

    }

    @Test
    public void map() throws Exception {
        source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\sensort.txt");
        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());

        SingleOutputStreamOperator<Tuple2<Integer, String>> map1 = map.map(new MyRichMapFunction());

        map1.print();

        env.execute();

        //my map open
        //my map open
        //my map open
        //my map open
        //3> (2,sensor_1)
        //2> (1,sensor_1)
        //1> (0,sensor_7)
        //4> (3,sensor_1)
        //my map close
        //3> (2,sensor_6)
        //my map close
        //my map close
        //1> (0,sensor_10)
        //my map close
    }

    public static class MyRichMapFunction extends RichMapFunction<SensorReading, Tuple2<Integer, String>> {
        @Override
        public Tuple2<Integer, String> map(SensorReading value) throws Exception {
            return new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), value.getName());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("my map open");
            // 以下可以做一些初始化工作，例如建立一个和HDFS的连接
        }

        @Override
        public void close() throws Exception {
            System.out.println("my map close");
            // 以下做一些清理工作，例如断开和HDFS的连接
        }
    }
}
