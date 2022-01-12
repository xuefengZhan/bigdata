package No01_WordCount;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;

import org.apache.flink.api.java.tuple.Tuple2;


/**
 *  flink 批处理体验
 *
 *  批处理用的是DataSet API
 */
public class Flink01_WordCount_Batch {

    public static void main(String[] args) throws Exception {
        //todo 1.创建flink应用环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        //todo 2.创建Source
        String inputPath = "D:\\IdeaProjects\\bigdata\\flink\\src\\main\\resources\\hello.txt";
        DataSource<String> source = env.readTextFile(inputPath);

        //todo 3.
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne = source.flatMap(new MyFlatMapper());

        //todo 4.按照元组中第一个元素分组
        UnsortedGrouping<Tuple2<String, Integer>> tuple2UnsortedGrouping = wordToOne.groupBy(0);


        //todo 5.将tuple第二个元素做相加
        AggregateOperator<Tuple2<String, Integer>> sum = tuple2UnsortedGrouping.sum(1);

        sum.print();
        //(scala,1)
        //(flink,1)
        //(world,1)
        //(hello,3)
    }


}
