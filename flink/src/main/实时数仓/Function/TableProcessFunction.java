package Function;

import Bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mysql.cj.xdevapi.Table;
import common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private Connection connection;
    private final OutputTag<JSONObject> HbaseOutputTag;
    private final MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private PreparedStatement preparedStatement;

    public TableProcessFunction(OutputTag<JSONObject> hbaseOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        HbaseOutputTag = hbaseOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //1.从主流中获取key 和 找配置表中的配置
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = jsonObject.getString("tableName") + jsonObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);

        //tableProcess != null
        if(tableProcess != null){
            //1. 保留需要的字段
            JSONObject data = jsonObject.getJSONObject("after");
            filterColumn(data,tableProcess.getSinkColumns());

            //2.分流  事实表从主流输出 然后主流写到kafka  维表从测流输出写到hbase
            String sinkType = tableProcess.getSinkType();
            data.put("sinkType",sinkType); //写出去后知道自己去哪
            if(TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                collector.collect(jsonObject);
            }else if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                //Hbase数据
                readOnlyContext.output(HbaseOutputTag,jsonObject);
            }

        }else{
            System.out.println(key + "不存在");
        }


    }

    @Override
    public void processBroadcastElement(String s, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //1.获取从配置表中读取出来的数据  解析成一个TableProcess对象
        JSONObject jsonObject = JSON.parseObject(s);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //2.HBase建表
        if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.广播
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key  = tableProcess.getSourceTable()+tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);
    }


    private void filterColumn( JSONObject data,String sinkColumns){
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);
        //移除不需要的字段
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }

    //拼接建表sql
    private void checkTable(String sinkTable,String sinkColumns,String PK,String extend){
        if(PK == null){
            PK = "id";
        }
        if(extend == null){
            extend = "";
        }

        StringBuffer sql = new StringBuffer("create table if not exists")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            if(PK.equals(field)){
                sql.append(field).append(" varchar primary key");
            }else{
                sql.append(field).append(" varchar");
            }
            //判断是否是最后一个field
            if(i != fields.length - 1){
                sql.append(")");
            }else{
                sql.append(",");
            }

            sql.append(extend);
        }

        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("phoenix 建表失败");
        }finally{
            if( preparedStatement != null){
                try{
                    preparedStatement.close();
                }catch (SQLException e){
                    e.printStackTrace();
                }
            }
        }

    }

}
