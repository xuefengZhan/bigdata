package No10_Checkpoint;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;

import java.util.List;

public class OperateState_ListCheckpoint {
    public static void main(String[] args) {

    }



    public class myMap implements MapFunction<String, Integer>, ListCheckpointed {

        @Override
        public Integer map(String value) throws Exception {
            return null;
        }

        @Override
        public List snapshotState(long checkpointId, long timestamp) throws Exception {
            return null;
        }

        @Override
        public void restoreState(List state) throws Exception {

        }
    }
}
