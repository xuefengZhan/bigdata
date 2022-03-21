package app.dwm;

import Util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.text.SimpleDateFormat;

public class UVApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 1.获取环境


        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        env.enableCheckpointing(5000l);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(10000);


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,1000));

        //todo 2.读取kafka dwd_page_log 主体数据


        String sourceTopic = "dwd_page_log";
        String groupID = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupID));
        //todo 3.转为Json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //todo 4.过滤数据 状态变成 保留每个mid当天第一次登录
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<JSONObject> filter = midKeyedDS.filter(new RichFilterFunction<JSONObject>() {
            //状态用于存储日期
            private ValueState<String> dateState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDesc = new ValueStateDescriptor<>(
                        "date",
                        String.class
                );
                //todo 设置TTL   创建和更新状态的时候重新定义TTL
                StateTtlConfig build = StateTtlConfig
                        .newBuilder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDesc.enableTimeToLive(build);
                dateState = getRuntimeContext().getState(
                        stateDesc
                );

                sdf = new SimpleDateFormat("yyyy-MM-dd");


            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                //如果lastPageId 存在，直接过滤掉
                if (lastPageId == null || lastPageId.length() <= 0) {
                    //获取上一次的日期和这一次的日期
                    String lastDate = dateState.value();
                    String curDate = sdf.format(jsonObject.getLong("ts"));

                    if (!curDate.equals(lastDate)) {
                        dateState.update(curDate);
                        return true;
                    }
                }
                return false;
            }
        });

        //todo 5.将数据写到kafka
        filter.map(JSONAware::toJSONString).addSink(KafkaUtil.getKafkaProducer(sinkTopic));

        env.execute();

    }

}
