package No03_transform;

import Bean.SensorReading;
import No03_transform.myFunction.MyFlatMapFunction;
import No03_transform.myFunction.MyMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class Flink01_map_flatMap_filter {
    StreamExecutionEnvironment  env;
    DataStreamSource<String> source;
    @Before
    public void init(){
       env = StreamExecutionEnvironment.getExecutionEnvironment();
       env.setParallelism(4);

    }

    @Test
    public void map()  {
        source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\sensort.txt");
        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());
        map.print();

    }

    @Test
    public void flatMap(){
        source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\hello.txt");
        SingleOutputStreamOperator<String> flatMap = source.flatMap(new MyFlatMapFunction());
        flatMap.print();
    }


    @Test
    public void filter(){
        source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\hello.txt");
        SingleOutputStreamOperator<String> scala = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !s.contains("scala");
            }
        });

        scala.print();
    }

    @After
    public void end() throws Exception {
        env.execute();
    }

}
