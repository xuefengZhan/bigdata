//package No03_transform;
//
//import Bean.SensorReading;
//import akka.japi.tuple.Tuple3;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.ReduceFunction;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.*;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.CoMapFunction;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.Collections;
//
//public class tarnsform03 {
//    StreamExecutionEnvironment env;
//    DataStreamSource<String> source;
//    @Before
//    public void init(){
//        env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);
//        source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\sensort.txt");
//
//    }
//
//
//    //需求：传感器数据按照温度高低（以30度为界），拆分成两个流
//    @Test
//    public void Split()  {
//
//        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());
//
//
//        SplitStream<SensorReading> splitStream = map.split(new OutputSelector<SensorReading>() {
//            @Override
//            public Iterable<String> select(SensorReading sensorReading) {
//                return (sensorReading.getTemp() > 30) ?
//                        Collections.singletonList("high") : Collections.singletonList("low");
//            }
//        });
//
//
//        //  从splitStream中选出DataStream
//        DataStream<SensorReading> highTempStream = splitStream.select("high");
//        DataStream<SensorReading> lowTempStream = splitStream.select("low");
//        DataStream<SensorReading> allTempStream = splitStream.select("high", "low");
//
//
//        highTempStream.print("high");
//        lowTempStream.print("low");
//        allTempStream.print("all");
//
//    }
//
//    @Test
//    public void connect(){
//
//
//            SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());
//
//
//            SplitStream<SensorReading> splitStream = map.split(new OutputSelector<SensorReading>() {
//                @Override
//                public Iterable<String> select(SensorReading sensorReading) {
//                    return (sensorReading.getTemp() > 30) ?
//                            Collections.singletonList("high") : Collections.singletonList("low");
//                }
//            });
//
//
//
//            DataStream<SensorReading> highTempStream = splitStream.select("high");
//            DataStream<SensorReading> lowTempStream = splitStream.select("low");
//
//
//            SingleOutputStreamOperator<Tuple3<String, Long, Double>> high = highTempStream.map(new MapFunction<SensorReading, Tuple3<String, Long, Double>>() {
//                @Override
//                public Tuple3<String, Long, Double> map(SensorReading s) throws Exception {
//                    return new Tuple3<>(s.getName(), s.getTs(), s.getTemp());
//                }
//            });
//
//
//            //todo 连接   一个流中有两个独立的流 二者可以不同类型
//            ConnectedStreams<Tuple3<String, Long, Double>, SensorReading> connectedStreams = high.connect(lowTempStream);
//
//            //todo
//            SingleOutputStreamOperator<Object> map1 = connectedStreams.map(new CoMapFunction<Tuple3<String, Long, Double>, SensorReading, Object>() {
//                @Override
//                public Object map1(Tuple3<String, Long, Double> tuple3) throws Exception {
//                    return new Tuple2<>(tuple3, "tuple");
//                }
//
//                @Override
//                public Object map2(SensorReading sensor) throws Exception {
//                    return new Tuple2<>(sensor, "sensor");
//                }
//            });
//
//         map1.print();
//    }
//
//
//
//    @After
//    public void end() throws Exception {
//        env.execute();
//    }
//}
