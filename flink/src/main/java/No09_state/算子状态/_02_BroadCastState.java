package No09_state.算子状态;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class _02_BroadCastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //todo 1.两个流
        DataStreamSource<String> source1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop102", 8888);


        //todo 2.定义状态并广播
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state",
                String.class,
                String.class
        );
        //广播出去
        BroadcastStream<String> broadcast = source1.broadcast(mapStateDescriptor);


        //todo 3.连接数据 和 广播流  构成： BroadcastConnectedStream
        BroadcastConnectedStream<String, String> connect = source2.connect(broadcast);


        //todo 4.处理连接后的流
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String s,  ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
                // 获取广播状态  获取另一条流的数据
                ReadOnlyBroadcastState<String, String> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
                String aSwitch = broadcastState.get("switch");

                if("1".equals(aSwitch)){
                    collector.collect("读取了广播状态，切换1");
                }else if("2".equals(aSwitch)){
                    collector.collect("读取了广播状态，切换2");
                }else if("3".equals(aSwitch)){
                    collector.collect("读取了广播状态，切换3");
                }


            }

            @Override
            public void processBroadcastElement(String s,  Context context, Collector<String> collector) throws Exception {
                BroadcastState<String, String> broadcastState = context.getBroadcastState(mapStateDescriptor);
                //处理广播流的输入数据，放进广播状态中
                broadcastState.put("switch",s);
            }
        }).print();

        env.execute();
    }


    //端口8888 输入1
    //打印结果：
    //3> 读取了广播状态，切换1
    //2> 读取了广播状态，切换1
    //1> 读取了广播状态，切换1

    //说明当前流每个并行度都能读取到广播状态
}
