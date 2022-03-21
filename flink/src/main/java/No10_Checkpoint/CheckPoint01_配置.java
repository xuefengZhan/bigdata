package No10_Checkpoint;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckPoint01_配置 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 1.开启ck 每隔1000ms开启一次checkpoint  默认情况下，Checkpoint机制是关闭的
        env.enableCheckpointing(1000);
        //env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        //env.enableCheckpointing(1000,CheckpointingMode.AT_LEAST_ONCE);

        //todo 2.设置一致性语义  只有at-least-once和 at-exactly-once
        // 默认的Checkpoint配置是支持Exactly-Once投递的
        // 使用Exactly-Once就是进行了Checkpoint Barrier对齐，因此会有一定的延迟。
        // 如果要求作业延迟小，那么应该使用At-Least-Once投递，不进行对齐，但某些数据会被处理多次。
        // 想要端到端exactly-once，source和sink也要保证exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        //todo 3.设置两次checkpoint的最小时间间隔
        // ck1: 0ms   完成时间：700ms   本来还剩300ms 再次执行一次ck
        // 这个参数就是限制上一次ck结束时间 和 下一次ck开始时间的最小间隔
        // 也就是说本来下一次ck的启动时间是 1000ms,有了这个参数之后，ck2的启动时间 = 700ms + 500ms = 1200ms
        // 保证整个作业最多允许1个Checkpoint
        // 如果不设置这个参数，假设状态很大，完成时间比较长：ck1在1200ms完成的，但是ck2在1000ms就开启了，因此有两个checkpoint同时进行
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);


        //todo 4.如果一次Checkpoint超过一定时间仍未完成，直接将其终止，以免其占用太多资源
        // 超时失败没问题 只要下一次ck成功即可
        env.getCheckpointConfig().setCheckpointTimeout(3600*1000);

        //todo 5.最大同时checkpoint个数
        // 默认情况下一个作业只允许1个Checkpoint执行
        // 如果某个Checkpoint正在进行，另外一个Checkpoint被启动，新的Checkpoint需要挂起等待。
        // 这个参数大于1，将与第3个配置 最短间隔相冲突。
        // 开启多了影响程序性能
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(3);

        //todo 6.作业cancel 仍然保留checkpoint
        // 作业取消后仍然保存Checkpoint
        // Checkpoint的初衷是用来进行故障恢复，如果作业是因为异常而失败，Flink会保存远程存储上的数据；
        // 如果开发者自己取消了作业，远程存储上的数据都会被删除。如果开发者希望通过Checkpoint数据进行调试，自己取消了作业，同时希望将远程数据保存下来
        // 此模式下，用户需要自己手动删除远程存储上的Checkpoint数据。
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        //todo 7.默认情况下，如果Checkpoint过程失败，会导致整个应用重启，我们可以关闭这个功能，这样Checkpoint失败不影响作业的运行。
        // checkpoint 线程是异步的，ck失败了，job接着运行
        // 如果关闭报错 下次ck成功 则没问题  下次ck失败 故障重启 可能会丢失数据或者数据重复
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);


    }
}
