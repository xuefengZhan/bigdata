package app.dwd;

import Bean.TableProcess;
import Function.DimSinkFunction;
import Function.TableProcessFunction;
import Util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;


public class dwd02_BaseDBApp {
    public static void main(String[] args) throws Exception {
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

        //todo 3.消费kafka  主流
        String topic = "ods_base_db";
        String groupID = "base_db_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupID));

        SingleOutputStreamOperator<JSONObject> jsobObjectDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String type = jsonObject.getString("type");
                        return !"delete".equals(type);
                    }
                });

        //todo 4.使用flinkCDC消费配置表 并处理成广播流
        String topicPro = "properties";
        String groupIDPro = "peizhi";
        // step1  创建了流
        DataStreamSource<String> ProDS = env.addSource(KafkaUtil.getKafkaConsumer(topicPro, groupIDPro));
        // step2  创建MapStateDescriptor  key为 广播流和主流join的字段
        MapStateDescriptor<String, TableProcess> brocastMap = new MapStateDescriptor<>("map-state", String.class,
                TableProcess.class);
        // step3  将流广播出去
        BroadcastStream<String> broadcastStream = ProDS.broadcast(brocastMap);

        //todo 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connect = jsobObjectDS.connect(broadcastStream);

        //todo 6. 分流 处理数据
        // 广播流 : 存入状态
        // 主流: 从状态取出数据 join

        OutputTag<JSONObject> HbaseSink = new OutputTag<JSONObject>("hbase") {};
        SingleOutputStreamOperator<JSONObject> kafkaSinkDS = connect.process(new TableProcessFunction(HbaseSink,brocastMap));


        //todo 7.提取kafka流数据和Hbase流数据
        DataStream<JSONObject> hbaseSink = kafkaSinkDS.getSideOutput(HbaseSink);

        //todo 8.将kafka数据写到kafka主流，将Hbase数据写入Phoenix
        hbaseSink.addSink(new DimSinkFunction());

        //实现动态topic  这里用sinkTable字段作为topic名称
        kafkaSinkDS.addSink(KafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(
                        jsonObject.getString("sinkTable"),
                        jsonObject.getString("after").getBytes()
                );
            }
        }));
        //todo 9.启动
        env.execute();
    }



}
