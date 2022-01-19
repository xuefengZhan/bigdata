package No03_transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

//todo
//  FlatMapFunction<T, O>
//  flatMap方法是void 返回值可以为0个也可以为多个
//  输出由collector控制
public class MyFlatMapFunction implements FlatMapFunction<String,String> {

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] strs = s.split(" ");
        for (String str : strs) {
            collector.collect(str);
        }
    }
}
