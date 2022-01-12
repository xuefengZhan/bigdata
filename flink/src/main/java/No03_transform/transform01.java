package No03_transform;

import Bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple3;

public class transform01 {
    StreamExecutionEnvironment  env;
    DataStreamSource<String> source;
    @Before
    public void init(){
       env = StreamExecutionEnvironment.getExecutionEnvironment();
       env.setParallelism(4);
        source = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort.txt");
    }

    @Test
    public void map()  {

        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());
        map.print();

    }

    @After
    public void end() throws Exception {
        env.execute();
    }

}
