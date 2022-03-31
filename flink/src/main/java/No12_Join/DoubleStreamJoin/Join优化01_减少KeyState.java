package No12_Join.DoubleStreamJoin;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class Join优化01_减少KeyState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new SourceFunction<Object>() {
                    @Override
                    public void run(SourceContext<Object> ctx) throws Exception {

                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(new KeySelector<Object, Object>() {
                    @Override
                    public Object getKey(Object value) throws Exception {
                        return null;
                    }
                })
                .connect(env.addSource(new SourceFunction<Object>() {
                    @Override
                    public void run(SourceContext<Object> ctx) throws Exception {

                    }

                    @Override
                    public void cancel() {

                    }
                }).keyBy(new KeySelector<Object, Object>() {
                    @Override
                    public Object getKey(Object value) throws Exception {
                        return null;
                    }
                }))
                // 左右两条流的数据
                .process(new KeyedCoProcessFunction<Object, Object, Object, Object>() {
                    // 两条流的数据共享一个 mapstate 进行处理
                    private transient MapState<String, String> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        this.mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("a", String.class, String.class));
                    }

                    @Override
                    public void processElement1(Object value, Context ctx, Collector<Object> out) throws Exception {

                    }

                    @Override
                    public void processElement2(Object value, Context ctx, Collector<Object> out) throws Exception {

                    }
                })
                .print();
    }
}
