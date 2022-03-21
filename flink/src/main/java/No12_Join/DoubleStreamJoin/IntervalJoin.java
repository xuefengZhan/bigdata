package No12_Join.DoubleStreamJoin;

import No12_Join.Bean.JoinBean;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 只支持eventTime 和 keyedStream
 */
public class IntervalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //todo 1.准备两个流
        DataStreamSource<String> DS1 = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<JoinBean> joinBeanDS1 = DS1.map(new MapFunction<String, JoinBean>() {
            @Override
            public JoinBean map(String s) throws Exception {
                String[] fields = s.split(",");
                return new JoinBean(fields[0], fields[1], Long.parseLong(fields[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<JoinBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JoinBean>() {
                            @Override
                            public long extractTimestamp(JoinBean joinBean, long l) {
                                return joinBean.getTs() * 1000;
                            }
                        })
        );

        DataStreamSource<String> DS2 = env.socketTextStream("hadoop102", 8888);
        SingleOutputStreamOperator<JoinBean> joinBeanDS2 = DS2.map(new MapFunction<String, JoinBean>() {
            @Override
            public JoinBean map(String s) throws Exception {
                String[] fields = s.split(",");
                return new JoinBean(fields[0], fields[1], Long.parseLong(fields[2]));
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<JoinBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JoinBean>() {
                            @Override
                            public long extractTimestamp(JoinBean joinBean, long l) {
                                return joinBean.getTs() * 1000;
                            }
                        })
        );
        
        //todo 双流join
        SingleOutputStreamOperator<Tuple2<JoinBean, JoinBean>> joinDS = joinBeanDS1.keyBy(x -> x.getName())
                .intervalJoin(joinBeanDS1.keyBy(x -> x.getName()))
                .between(Time.seconds(-5), Time.seconds(5))//ds2等ds1 ds1等ds2
                //.lowerBoundExclusive()
                //.upperBoundExclusive()
                .process(new ProcessJoinFunction<JoinBean, JoinBean, Tuple2<JoinBean, JoinBean>>() {
                    @Override
                    public void processElement(JoinBean joinBean, JoinBean joinBean2, ProcessJoinFunction<JoinBean, JoinBean, Tuple2<JoinBean, JoinBean>>.Context context, Collector<Tuple2<JoinBean, JoinBean>> collector) throws Exception {
                        collector.collect(new Tuple2<>(joinBean, joinBean2));
                    }
                });

        joinDS.print();

        env.execute();
        //8888           9999
        //1001  1
        //               1001  1    =>输出 1001 1 , 1001  1
        //               1001  2    =>输出 1001 1 ,  1001 2
        //1002 10
        //               1001 3     =>输出 1001 1 ,  1001 3
        //               1002 10    =>输出 1002 10,  1000 10
        //               1001 3     =>不输出  因为watermark已经到10了
    }
}
