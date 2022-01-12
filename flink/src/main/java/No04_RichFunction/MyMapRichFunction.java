package No04_RichFunction;

import Bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class MyMapRichFunction extends RichMapFunction<SensorReading, Tuple2<Integer, String>> {
    @Override
    public Tuple2<Integer, String> map(SensorReading value) throws Exception {
        return new Tuple2<>(getRuntimeContext().getIndexOfThisSubtask(), value.getName());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("my map open");
        // 以下可以做一些初始化工作，例如建立一个和HDFS的连接
    }

    @Override
    public void close() throws Exception {
        System.out.println("my map close");
        // 以下做一些清理工作，例如断开和HDFS的连接
    }
}
