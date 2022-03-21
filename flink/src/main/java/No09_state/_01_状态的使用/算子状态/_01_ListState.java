package No09_state._01_状态的使用.算子状态;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

public class _01_ListState {
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
        private ListState<Integer> listState;


        @Override
        public Integer map(String line ) throws Exception {
            Iterator<Integer> iterator = listState.get().iterator();
            if(!iterator.hasNext()){
                listState.add(1);
            }else{
                Integer count = iterator.next();
                listState.clear();
                listState.add(count + 1);
            }

            return listState.get().iterator().next();
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

        }

        //todo 做checkpoint的时候会调用这个方法
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {


        }
    }


}
