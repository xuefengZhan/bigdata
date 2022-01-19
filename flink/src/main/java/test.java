import Bean.SensorReading;
import No03_transform.MyMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<String> source = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\sensort.txt");

        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());


        KeyedStream<SensorReading, String> keyedStream = map.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getName();
            }
        });


        WindowedStream<SensorReading, String, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));


//        windowedStream.reduce(new ReduceFunction<SensorReading>() {
//            @Override
//            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
//                return null;
//            }
//        });



        // 求每个sensor的平均温度
        SingleOutputStreamOperator<Double> aggregate = windowedStream.aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {

            @Override
            public Tuple2<Double, Integer> createAccumulator() {
                return new Tuple2<>(0.0,0);
            }

            @Override
            public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {

                double allTemp = accumulator.getField(0);

                return new Tuple2<>(value.getTemp() + allTemp, (int) accumulator.getField(1) + 1);
            }

            @Override
            public Double getResult(Tuple2<Double, Integer> accumulator) {
                return (double) accumulator.getField(0) / (int) accumulator.getField(1);
            }

            @Override
            public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                return new Tuple2<>((double) a.getField(0) + (double) b.getField(0), (int) a.getField(1) + (int) b.getField(1));
            }
        });


        aggregate.print();
        env.execute();
    }
}
