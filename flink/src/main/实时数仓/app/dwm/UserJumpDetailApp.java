package app.dwm;

import Util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //1.获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 1.获取环境
        env.setParallelism(1);//生产环境中和kafka分区数宝池一致

        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000l);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(10000);


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,1000));

        //2.读取kafka
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //todo 3.转为json 提取时间戳 设置watermark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObject, long l) {
                                        return jsonObject.getLong("ts");
                                    }
                                }));

        //todo 4.CEP 定义模式序列
        //1.第一个事件的last_page_id == null 并且 第二个事件的last_page_id == null
        //2.第一个事件的last_page_id == null  并且 10s内没有第二个事件
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).within(Time.seconds(10));

        //序列模式的第二种定义方式
        Pattern<JSONObject, JSONObject> pattern2 = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).times(2).consecutive()//严格近邻
                .within(Time.seconds(10));


        //todo 5.将cep定义在流上
        PatternStream<JSONObject> pattern1 = CEP.pattern(jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"))
                , pattern);

        //todo 6.提取匹配上的 和 超时事件
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeout") {
        };
        //1.匹配上的
        SingleOutputStreamOperator<JSONObject> selectDS = pattern1.select(timeOutTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                return map.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                return map.get("start").get(0);
            }
        });
        //2.超时的
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //todo 7.union两种符合条件的
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        //todo 8.将数据写道kafka
        unionDS.map(JSONAware::toJSONString)
                .addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        //todo 9.启动任务
        env.execute();


    }
    //测试：
    // 跳出的第一种定义：第一个事件的last_page_id == null 并且 第二个事件的last_page_id == null，说明第一条是跳出
    // last_page_id:null  70000
    // last_page_id:null  75000  没有输出，因为用的是事件时间，waterMark = 2s，
    // 由于pattern定义的是next严格近邻，75000=》wm = 73000  此时可能再来一条数据是74000 这样的话74000和70000才是严格近邻
    // 因此第二条不会触发输出
    // 此时如果输入一条 last_page_id null  74000 会输出第一条
    // 或者 last_page_id null  77000 以上的 会输出第一条

    // 跳出的第二种定义：第一个事件的last_page_id == null  并且 10s内没有第二个事件
    // last_page_id:null  70000
    // last_page_id 有或没有都无所谓  7000 + 10000 + 2000   期间没有数据进来  此时输出第一条
}
