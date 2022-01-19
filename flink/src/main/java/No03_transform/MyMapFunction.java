package No03_transform;

import Bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;


//todo  MapFunction<T, O>    必须要有返回值
public class MyMapFunction implements MapFunction<String, SensorReading> {



    @Override
    public SensorReading map(String s) throws Exception {
        String[] split = s.split(",");
        String name = split[0].trim();
        long ts = Long.parseLong(split[1].trim());
        double temp = Double.parseDouble(split[2].trim());

        return new SensorReading(name,ts,temp);
    }
}
