package No12_Join.DoubleStreamJoin;

import No12_Join.Bean.JoinBean;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 *  InnerJoin 是两个六
 */
public class WindowJoin01_InnerJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceDS1 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> sourceDS2= env.socketTextStream("hadoop102", 9999);


        SingleOutputStreamOperator<JoinBean> joinBean1 = sourceDS1.map(x -> {
            String[] fields = x.split(" ");
            return new JoinBean(fields[0], fields[1], Long.parseLong(fields[2]));
        });

        SingleOutputStreamOperator<JoinBean> joinBean2 = sourceDS2.map(x -> {
            String[] fields = x.split(" ");
            return new JoinBean(fields[0], fields[1], Long.parseLong(fields[2]));
        });


        DataStream<Tuple2<JoinBean, JoinBean>> res = joinBean1.join(joinBean2)
                .where(JoinBean::getName)
                .equalTo(JoinBean::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<JoinBean, JoinBean, Tuple2<JoinBean, JoinBean>>() {
                    // 窗口中关联到的数据的处理逻辑
                    // 只支持 inner join，即窗口内能关联到的才会下发，关联不到的则直接丢掉。
                    @Override
                    public Tuple2<JoinBean, JoinBean> join(JoinBean first, JoinBean second) throws Exception {
                        return new Tuple2(first, second);
                    }
                });
    }
}
