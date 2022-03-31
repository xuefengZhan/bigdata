package No12_Join.DoubleStreamJoin;

import No12_Join.Bean.JoinBean;
import org.apache.calcite.rel.core.Join;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 *  coGroup 算子
 *  可以实现 inner join 、 outer join
 */
public class WindowJoin02_OuterJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceDS1 = env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\joinbean1.txt");
        DataStreamSource<String> sourceDS2= env.readTextFile("E:\\work\\bigdata\\flink\\src\\main\\resources\\joinbean2.txt");


        SingleOutputStreamOperator<JoinBean> joinBean1 = sourceDS1.map(x -> {
            String[] fields = x.split(" ");
            return new JoinBean(fields[0], fields[1], Long.parseLong(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JoinBean>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JoinBean element) {
                return element.getTs();
            }
        });

        SingleOutputStreamOperator<JoinBean> joinBean2 = sourceDS2.map(x -> {
            String[] fields = x.split(" ");
            return new JoinBean(fields[0], fields[1], Long.parseLong(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JoinBean>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(JoinBean element) {
                return element.getTs();
            }
        });


        DataStream<Tuple2<JoinBean, JoinBean>> res = joinBean1.coGroup(joinBean2)
                .where(JoinBean::getName)
                .equalTo(JoinBean::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<JoinBean, JoinBean, Tuple2<JoinBean, JoinBean>>() {


                    @Override
                    public void coGroup(Iterable<JoinBean> first, Iterable<JoinBean> second, Collector<Tuple2<JoinBean, JoinBean>> out) throws Exception {
                        JoinBean joinBean1 = new JoinBean("替补1", "20", 1648437586000l);
                        JoinBean joinBean2 = new JoinBean("替补2", "20", 1648437586000l);

                        Iterator<JoinBean> iterator1 = first.iterator();
                        Iterator<JoinBean> iterator2 = second.iterator();


                        if (!iterator1.hasNext() && !iterator2.hasNext()) {
                            out.collect(new Tuple2<>(joinBean1, joinBean2));
                        } else if (!iterator1.hasNext()) {
                            while (iterator2.hasNext()) {
                                out.collect(new Tuple2<>(joinBean1, iterator2.next()));
                            }
                        } else if (!iterator2.hasNext()) {
                            while (iterator1.hasNext()) {
                                out.collect(new Tuple2<>(iterator1.next(), joinBean2));
                            }
                        } else {
                            while (iterator1.hasNext()) {
                                while (iterator2.hasNext()) {
                                    out.collect(new Tuple2<>(iterator1.next(), iterator2.next()));
                                }
                            }
                        }


                    }
                });


        res.print();

        env.execute();

        //鸣人 男 1648437586000
        //佐助 男 1648437587000
        //小樱 女 1648437588000

        //鸣人 男 1648437586000
        //小樱 女 1648437588000
        //左井 男 1648437588000

        //6> (JoinBean{name='替补1', age='20', ts=1648437586000},JoinBean{name='左井', age='男', ts=1648437588000})
        //1> (JoinBean{name='小樱', age='女', ts=1648437588000},JoinBean{name='小樱', age='女', ts=1648437588000})
        //1> (JoinBean{name='佐助', age='男', ts=1648437587000},JoinBean{name='替补2', age='20', ts=1648437586000})
        //3> (JoinBean{name='鸣人', age='男', ts=1648437586000},JoinBean{name='鸣人', age='男', ts=1648437586000})
    }
}
