package No12_Join.DimJoin;


import No12_Join.Bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

/**
 * 通过定义一个类实现RichMapFunction，在open()中读取维表数据加载到内存中，在probe流map()方法中与维表数据进行关联。
 * RichMapFunction中open方法里加载维表数据到内存的方式特点如下：
 * 优点：实现简单
 * 缺点：因为数据存于内存，所以只适合小数据量并且维表数据更新频率不高的情况下。虽然可以在open中定义一个定时器定时更新维表，但是还是存在维表更新不及时的情况。
 *
 * 用户表：   username cityid timestamp
 * 城市维表： cityid cityname timestamp
 *
 *
 * 输出：用户名称、城市ID、城市名称
 */

public class PreLoadDimTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 1.主流
        DataStreamSource<String> userSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> textStream  = userSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(" ");
                return new Tuple2<String, Integer>(fields[0], Integer.valueOf(fields[1]));
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        });

        //todo 2.join 维表
        SingleOutputStreamOperator<Tuple3<String, Integer, String>> res = textStream.flatMap(new myMapFunc());

        res.print();

        env.execute();


    }

    private static class myMapFunc extends RichFlatMapFunction<Tuple2<String,Integer>, Tuple3<String,Integer,String>>{

        HashMap<Integer,String> city =  new HashMap<Integer,String>();

        @Override
        public void open(Configuration parameters) throws Exception {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = "jdbc:mysql://localhost:3306/niuke?serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=utf8&autoReconnect=true&useSSL=false";
            String username = "root";
            String password = "jkl;";
            Connection connection = DriverManager.getConnection(url, username, password);

            String sql = "select cityid,cityname from city;";

            PreparedStatement s = connection.prepareStatement(sql);

            ResultSet resultSet = s.executeQuery();
            while(resultSet.next()){
                int cityid = resultSet.getInt("cityid");
                String cityName = resultSet.getString("cityname");

                city.put(cityid,cityName);
            }

            connection.close();
        }

        @Override
        public void flatMap(Tuple2<String, Integer> t2, Collector<Tuple3<String, Integer, String>> collector) throws Exception {
            Integer cityid = t2.f1;
            if(city.containsKey(cityid)){
                collector.collect(new Tuple3<>(t2.f0,cityid,city.get(cityid)));
            }
        }
    }
}

//Mysql 建表
//CREATE TABLE city(
// cityid INT,
// cityname VARCHAR(20),
// `timestamp` TIMESTAMP
//)
//
//INSERT INTO city VALUES(1,'杭州','2022-03-10 22:33:00');
//INSERT INTO city VALUES(2,'武汉','2022-03-10 22:33:00');
//INSERT INTO city VALUES(3,'合肥','2022-03-10 22:33:00');


//输入测试：
//詹学丰 1 2022-03-10 22:33:00
//5> (詹学丰,1,杭州)
//詹学丰 1 2022-03-10 22:33:00
//6> (詹学丰,1,杭州)