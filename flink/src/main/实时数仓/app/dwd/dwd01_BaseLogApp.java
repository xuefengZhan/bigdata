package app.dwd;

import Util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class dwd01_BaseLogApp {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 1.设置ck
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000l);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        //todo 2.设置恢复
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,1000));

        //todo 3.消费kafka
        String topic = "ods_base_log";
        String groupID = "baselogapp";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupID));

        //todo 4.将每行数据转换成JSON   脏数据测输出流写出  防止程序挂掉
        //定义测输出流
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {};
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {

            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(outputTag, s);
                }
            }
        });

        //todo 5.新老用户校验  用户根据mid
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = jsonDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("isNew", Integer.class)
                        );
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) { //表示认为是新用户
                            Integer state = valueState.value();
                            if (state != null) { //说明前端是错的，这个不是新用户
                                jsonObject.getJSONObject("common").put("is_new", 0);
                            } else { //确实是新用户  将状态更新为老用户
                                valueState.update(1);
                            }
                        }

                        return jsonObject;
                    }
                });

        //todo 6.分流 按照不同的日志类型写入不同的topic  测输出流 只能用process
        //页面 主流  启动 和 曝光测输出流
        OutputTag<String> startOutputTag = new OutputTag<String>("start") {};
        OutputTag<String> displayOutputTag = new OutputTag<String>("display") {};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                        String start = jsonObject.getString("start");
                        if (start != null && start.length() > 0) {
                            context.output(startOutputTag, jsonObject.toJSONString());
                        } else {

                            JSONArray displays = jsonObject.getJSONArray("displays");
                            //曝光列表  压平输出
                            if (displays != null && displays.size() > 0) {
                                String pageID = jsonObject.getJSONObject("page").getString("page_id");
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject display = displays.getJSONObject(i);
                                    display.put("page_id", pageID);

                                    context.output(displayOutputTag, display.toJSONString());
                                }
                            } else {
                                collector.collect(jsonObject.toJSONString());
                            }
                        }
                    }
                }
        );

        //todo 7.shuchu

        DataStream<String> startDS = pageDS.getSideOutput(startOutputTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayOutputTag);


        startDS.addSink(KafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(KafkaUtil.getKafkaProducer("dwd_display_log"));
        pageDS.addSink(KafkaUtil.getKafkaProducer("dwd_start_log"));
    }
}
