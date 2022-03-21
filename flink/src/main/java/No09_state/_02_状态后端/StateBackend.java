
package No09_state._02_状态后端;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class  StateBackend {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 设置状态后端
        // 默认情况下，状态保存在TaskManagers的内存中
        env.setStateBackend(new MemoryStateBackend(5242880,false));



        env.setStateBackend(new FsStateBackend("hdfs:hadoop102:8020/flink/ck"));
        //RocksDBBackend 需要导入额外的依赖
        //env.setStateBackend(new RicksDBStateBackend("hdfs:hadoop102:8020/flink/ck"));


        //todo 2.开启checkpoint
        env.enableCheckpointing();

    }
}
