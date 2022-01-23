package No06_Window;

import Bean.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.Tuple;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyWindowFunc implements WindowFunction<SensorReading, Tuple2<String,Integer>,Tuple, TimeWindow> {

    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple2<String, Integer>> out) throws Exception {
        String key = tuple.getField(0);
        //窗口所有数据都放在了迭代器中
        int size = IteratorUtils.toList(input.iterator()).size();

        //输出
        out.collect(new Tuple2<>(key,size));
    }
}
