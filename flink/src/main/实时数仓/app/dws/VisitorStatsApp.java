package app.dws;

import Bean.VisitorStats;
import Util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop102:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");

        String groupId = "visitor_stats_app";

        //TODO 1.从Kafka的pv、uv、跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";   //当日去重后的明细数据
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";  //

        FlinkKafkaConsumer<String> pageViewSource = KafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource =KafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = KafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);


        //todo 2.将每个流处理成相同的数据类型  方便做union
        //2.1 转换pv流  dwd_page_log 一条数据代表一个pv
        SingleOutputStreamOperator<VisitorStats> pageViewStatsDstream = pageViewDStream.map(
                json -> {
                    //  System.out.println("pv:"+json);
                    JSONObject jsonObj = JSON.parseObject(json);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 1L, 0L, 0L, jsonObj.getJSONObject("page").getLong("during_time"), jsonObj.getLong("ts"));
                });

        //2.2转换uv流  dwm_unique_visit 一条数据代表一次uv
        SingleOutputStreamOperator<VisitorStats> uniqueVisitStatsDstream = uniqueVisitDStream.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    return new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            1L, 0L, 0L, 0L, 0L, jsonObj.getLong("ts"));
                });

        //2.3 转换sv流  进入次数
        SingleOutputStreamOperator<VisitorStats> sessionVisitDstream = pageViewDStream.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String json, Context ctx, Collector<VisitorStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(json);
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            //    System.out.println("sc:"+json);
                            VisitorStats visitorStats = new VisitorStats("", "",
                                    jsonObj.getJSONObject("common").getString("vc"),
                                    jsonObj.getJSONObject("common").getString("ch"),
                                    jsonObj.getJSONObject("common").getString("ar"),
                                    jsonObj.getJSONObject("common").getString("is_new"),
                                    0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"));
                            out.collect(visitorStats);
                        }
                    }
                });

        //2.4 转换跳转流  dwm_user_jump_detail 一条数据代表一次uv
        SingleOutputStreamOperator<VisitorStats> userJumpStatDstream = userJumpDStream.map(json -> {
            JSONObject jsonObj = JSON.parseObject(json);
            return new VisitorStats("", "",
                    jsonObj.getJSONObject("common").getString("vc"),
                    jsonObj.getJSONObject("common").getString("ch"),
                    jsonObj.getJSONObject("common").getString("ar"),
                    jsonObj.getJSONObject("common").getString("is_new"),
                    0L, 0L, 0L, 1L, 0L, jsonObj.getLong("ts"));
        });

        //所有流转换数据类型的时候，用的都是自己的时间戳
        //pv -> dwd_page_log -> 访客中直接消费开窗 [10,20)
        //       uj CEP中定义10s后输出 ->
        // 想要保证uj数据也能进入窗口，需要将下面的水位线的waterMark设置更久点

       //TODO 3.将四条流合并起来
        DataStream<VisitorStats> unionDetailDstream = uniqueVisitStatsDstream.union(
                pageViewStatsDstream,
                sessionVisitDstream,
                userJumpStatDstream
        );


        //TODO 4.设置水位线
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDstream =
                unionDetailDstream.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11)).
                                withTimestampAssigner( (visitorStats,ts) ->visitorStats.getTs() )
                ) ;

        visitorStatsWithWatermarkDstream.print("after union:::");

        //TODO 5.分组 选取四个维度作为key , 使用Tuple4组合
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsTuple4KeyedStream =
                visitorStatsWithWatermarkDstream
                        .keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                                   @Override
                                   public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                                       return new Tuple4<>(visitorStats.getVc()
                                               , visitorStats.getCh(),
                                               visitorStats.getAr(),
                                               visitorStats.getIs_new());

                                   }
                               }
                        );
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowStream =
                visitorStatsTuple4KeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));


        //TODO 7.Reduce聚合统计
        // window的增量聚合 reduce 和 aggregate 都可以有windowFunction
        // process 和 apply都是全量聚合 可以获取window信息
        // 这里在windowFunction中将window的起始时间传递到javaBean中
        SingleOutputStreamOperator<VisitorStats> visitorStatsDstream =
                windowStream.reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        //把度量数据两两相加
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;
                    }
                }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    // 迭代器中只有一个元素
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, Context context,
                                        Iterable<VisitorStats> visitorStatsIn,
                                        Collector<VisitorStats> visitorStatsOut) throws Exception {
                        //补时间字段
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (VisitorStats visitorStats : visitorStatsIn) {

                            String startDate = simpleDateFormat.format(new Date(context.window().getStart()));
                            String endDate = simpleDateFormat.format(new Date(context.window().getEnd()));

                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            visitorStatsOut.collect(visitorStats);
                        }
                    }
                });
        visitorStatsDstream.print("reduce:");


        env.execute();
    }
}
