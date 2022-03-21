package No06_Window;

import Bean.SensorReading;
import No03_transform.myFunction.MyMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Window_test {
    StreamExecutionEnvironment env;
    DataStreamSource<String> source;
    KeyedStream<SensorReading, Tuple> key;
    @Before
    public void init(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\sensort.txt");
        SingleOutputStreamOperator<SensorReading> map = source.map(new MyMapFunction());


        key  = map.keyBy("name");
    }

    @Test
    public void createWindow()  {


        //todo 1.window()的参数是一个窗口分配器


        //todo 1.创建滚动窗口 通用方式
        WindowedStream<SensorReading, Tuple,  TimeWindow> window = key.window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
        //todo 2.创建滚动窗口方式2
        WindowedStream<SensorReading, Tuple,  TimeWindow> window2 = key.timeWindow(Time.seconds(15));


        //todo 1.创建滑动窗口方式 通用方式
        WindowedStream<SensorReading, Tuple,  TimeWindow> window3 = key.window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(5)));
        //todo 2.创建滑动窗口方式
        WindowedStream<SensorReading, Tuple,  TimeWindow> window4 = key.timeWindow(Time.seconds(15), Time.seconds(5));


        //todo 1.SessionWindow
        WindowedStream<SensorReading, Tuple,  TimeWindow> SessionWindow = key.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        //
        key.countWindow(5);
  }


    /**
     * 全量窗口函数
     */
  @Test
  public void all(){
      WindowedStream<SensorReading, Tuple, TimeWindow> windowedStream = key.timeWindow(Time.seconds(10));

      SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.apply(new MyWindowFunc());




  }

    @After
    public void end() throws Exception {
        env.execute();
    }
}
