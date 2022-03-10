package No10_Checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _02_重启恢复策略 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //todo 1.如果没有checkpoint 重启job就不会恢复数据

        env.enableCheckpointing(1000);
        // todo  恢复策略有三种：
        // todo  1.固定延迟策略   默认使用; 默认尝试重启次数 = Integer.MAX_VALUE
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.seconds(3)
        ));

        //todo 2.



    }
}
