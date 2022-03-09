package No09_state._01_状态的使用;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class _10_BroadcastState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //分别创建两个数据集
        ArrayList<Tuple2<Integer,String>> student = new ArrayList<>();
        student.add(new Tuple2<Integer,String>(1,"大胖"));
        student.add(new Tuple2<Integer,String>(2,"二狗子"));
        student.add(new Tuple2<Integer,String>(3,"三狗子"));
        student.add(new Tuple2<Integer,String>(4,"小鬼子"));
        student.add(new Tuple2<Integer,String>(5,"小丸子"));

        ArrayList<Tuple3<Integer,String,Integer>> score = new ArrayList<>();
        score.add(new Tuple3<>(1,"语文",50));
        score.add(new Tuple3<>(2,"数学",70));
        score.add(new Tuple3<>(3,"英文",86));
        score.add(new Tuple3<>(5,"物理",86));
        score.add(new Tuple3<>(6,"化学",86));


        DataStream<Tuple2<Integer, String>> studentStream = env.fromCollection(student).broadcast();
        DataStreamSource<Tuple3<Integer, String, Integer>> scoreStream = env.fromCollection(score);


    }

}
