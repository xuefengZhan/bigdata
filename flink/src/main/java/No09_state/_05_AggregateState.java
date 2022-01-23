package No09_state;

import Bean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class _05_AggregateState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        KeyedStream<SensorReading, Tuple> keyedStream = source.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] datas = s.split(",");
                return new SensorReading(datas[0], Long.valueOf(datas[1]), Double.valueOf(datas[2]));
            }
        }).keyBy("name");


         //todo 计算平均水位
        keyedStream.process(new KeyedProcessFunction<Tuple,SensorReading, Tuple2<String,Double>>() {

            //输入和输出可以不一样
            AggregatingState<SensorReading,Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggregatingState = getRuntimeContext().getAggregatingState(
                        new AggregatingStateDescriptor<SensorReading,Acc, Double>(
                                "acc-state",
                                new AggregateFunction<SensorReading, Acc, Double>() {
                                    @Override
                                    public Acc createAccumulator() {
                                        return new Acc(0,0.0);
                                    }

                                    @Override
                                    public Acc add(SensorReading sensorReading, Acc acc) {
                                        return new Acc(acc.getCnt() + 1,acc.getSum() + sensorReading.getTemp());
                                    }

                                    @Override
                                    public Double getResult(Acc acc) {
                                        return acc.getSum()/acc.getCnt();
                                    }

                                    @Override
                                    public Acc merge(Acc acc, Acc acc1) {
                                        return new Acc(acc.getCnt() + acc1.getCnt(),acc.getSum() + acc.getSum());
                                    }
                                },
                                Acc.class
                        )
                );
            }

            @Override
            public void processElement(SensorReading value, KeyedProcessFunction<Tuple, SensorReading, Tuple2<String, Double>>.Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                aggregatingState.add(value);

                Double res = aggregatingState.get();

                out.collect(new Tuple2<>(value.getName(),res));

            }
        }).print();

        env.execute();
    }

    public static class Acc{
        private Integer cnt;
        private Double sum;

        public Acc() {
        }

        public Acc(Integer cnt, Double sum) {
            this.cnt = cnt;
            this.sum = sum;
        }

        public Integer getCnt() {
            return cnt;
        }

        public void setCnt(Integer cnt) {
            this.cnt = cnt;
        }

        public Double getSum() {
            return sum;
        }

        public void setSum(Double sum) {
            this.sum = sum;
        }
    }
}
