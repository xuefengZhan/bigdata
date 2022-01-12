package No01_WordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>> {

    //输出类型是个Collector 收集器
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {

        String[] words = s.split(" ");

        for (String word : words) {
            Tuple2<String,Integer> res = new Tuple2<String,Integer>(word,1);
            collector.collect(res);
        }

    }
}
