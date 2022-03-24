package app.dws;

import Bean.ProvinceStats;
import Util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import ru.yandex.clickhouse.ClickHouseUtil;

public class ProvinceStatsSqlApp {






    public static void main(String[] args) {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 1.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2.把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        //DDL 用 executeSql
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING," +
                "province_iso_code STRING," +
                "province_3166_2_code STRING," +
                "order_id STRING, " +
                "total_amount DOUBLE," +
                "create_time STRING," +
                "rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND)" +
                " WITH (" +  KafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")");

       //TODO 3.聚合计算  分组 开窗 聚合
        //DML 用sqlQuery
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                " province_id,province_name," +
                "province_area_code," +
                "province_iso_code," +
                "province_3166_2_code," +
                "COUNT( DISTINCT order_id) order_count, " +
                "sum(total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts "+
                " from ORDER_WIDE " +
                "group by TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");


        //TODO 4.转换为数据流
        DataStream<ProvinceStats> provinceStatsDataStream =
                tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);


        //TODO 5.写入到lickHouse
//        provinceStatsDataStream.addSink(ClickHouseUtil.
//                <ProvinceStats>getJdbcSink("insert into  province_stats_2021  values(?,?,?,?,?,?,?,?,?,?)"));


    }



}
