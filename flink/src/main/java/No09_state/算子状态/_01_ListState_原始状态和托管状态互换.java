package No09_state.算子状态;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

public class _01_ListState_原始状态和托管状态互换 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\sensort.txt");

        source.map(new myMapFunc()).print();

        env.execute();

    }


    // 算子状态 - listState  实现功能： 统计元素个数
    // 算子状态必须要实现CheckpointedFunction 接口
    private static class myMapFunc implements MapFunction<String, Integer>,CheckpointedFunction {

        private ListState<Integer> listState;//托管状态
        int count; //原始状态

        @Override
        public Integer map(String line ) throws Exception {
            count ++;
            return count;
        }

        // 初始化状态的方法会有两次调用时机：
        // 1.第一次创建
        // 2.程序从checkpoint恢复
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // todo 初始化状态
            listState = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Integer>(
                            "list-state",
                            Integer.class
                    ));

            // todo 2.context.isRestored() => true 表示 状态恢复
            //  状态恢复的时候 状态不为空 将托管状态交给原始状态
            if(context.isRestored()){
                Iterable<Integer> integers = listState.get();
                count = integers.iterator().next();

            }

        }

        //todo 做checkpoint的时候会调用这个方法
        // 将原始状态 转换成 托管状态
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            listState.clear();
            listState.add(count);

        }
    }


}
