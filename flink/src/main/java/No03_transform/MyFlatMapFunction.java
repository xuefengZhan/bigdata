package No03_transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class MyFlatMapFunction implements FlatMapFunction<String,String> {

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] strs = s.split(" ");
        for (String str : strs) {
            collector.collect(str);
        }
    }
}
