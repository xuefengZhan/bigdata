package No09_state;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class _08_State_Backend {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置状态后端
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs:hadoop102:8020/flink/ck"));
        //RocksDBBackend 需要导入额外的依赖
        //env.setStateBackend(new RicksDBStateBackend("hdfs:hadoop102:8020/flink/ck"));


        //todo 2.开启checkpoint
        env.enableCheckpointing();

    }
}
