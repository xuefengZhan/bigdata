package No03_transform;

import Bean.SensorReading;
import No03_transform.myFunction.MyMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class tarnsform02 {
    StreamExecutionEnvironment env;
    DataStreamSource<String> source;
    @Before
    public void init(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\sensort.txt");

    }

    @Test
    public void keyBy()  {

        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());
        //1.keyBy 中写索引下标 只能用于元组类型

        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = map.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getName();
            }
        });

        //select max(temp) from xx group by name
        SingleOutputStreamOperator<SensorReading> name = sensorReadingStringKeyedStream.max("temp");
        name.print();

    }


    @Test
    public void reduce(){
        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());
        //1.keyBy 中写索引下标 只能用于元组类型

        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = map.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getName();
            }
        });


        //取最小的温度值，并输出当前的时间戳
        SingleOutputStreamOperator<SensorReading> reduce = sensorReadingStringKeyedStream.reduce(new ReduceFunction<SensorReading>() {
            /**
             *
             * @param s1 : 以前聚合的结果
             * @param s2 : 新来的一条数据
             * @return
             * @throws Exception
             */
            @Override
            public SensorReading reduce(SensorReading s1, SensorReading s2) throws Exception {
                return new SensorReading(s2.getName(), s2.getTs(), Math.min(s1.getTemp(), s2.getTemp()));

            }
        });

        reduce.print();
    }


    @After
    public void end() throws Exception {
        env.execute();
    }
}
