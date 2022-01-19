package No02_source;

import Bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Flink05_Source_diy {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从kafka中读取
        DataStreamSource<WaterSensor> inputDS = env.addSource(new MySource());

        inputDS.print();

        env.execute();
    }


    public static class MySource implements SourceFunction<WaterSensor> {
        boolean flag = true;

        @Override
        public void run( SourceContext<WaterSensor> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                ctx.collect(
                        new WaterSensor(
                                "sensor_" + (random.nextInt(3) + 1),
                                System.currentTimeMillis(),
                                random.nextInt(10) + 40
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }


}
