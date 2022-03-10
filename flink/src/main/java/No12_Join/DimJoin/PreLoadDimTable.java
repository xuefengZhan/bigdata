package No12_Join.DimJoin;


import No12_Join.Bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecCalc;

/**
 * 通过定义一个类实现RichMapFunction，在open()中读取维表数据加载到内存中，在probe流map()方法中与维表数据进行关联。
 * RichMapFunction中open方法里加载维表数据到内存的方式特点如下：
 * 优点：实现简单
 * 缺点：因为数据存于内存，所以只适合小数据量并且维表数据更新频率不高的情况下。虽然可以在open中定义一个定时器定时更新维表，但是还是存在维表更新不及时的情况。
 *
 * 用户表：   username cityid timestamp
 * 城市维表： cityid cityname timestamp
 */

public class PreLoadDimTable {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> userSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> textStream  = userSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(" ");
                return new Tuple2<String, Integer>(fields[0], Integer.valueOf(fields[1]));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        });


    }
}
