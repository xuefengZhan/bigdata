package Function;

import Util.DimUtil;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.calcite.prepare.Prepare;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        try{
            //sql语句

            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = getUpserSql(sinkTable, after);

            //预编译
            preparedStatement = connection.prepareStatement(upsertSql);
            //如果当前数据为更新，则先删除redis中数据,再写入Hbase
            //step1 .redis 中删除不存在的key 不会有问题
            if("update".equals(value.get("type"))){
                DimUtil.delRedisDimInfo( sinkTable ,after.getString("id"));
            }

            //这个sink属于BaseDBAPP进程中的
            //查询属于OrderWideAPP
            //在执行step2插入之前,orderWideAPP进行了查询，又将数据写到了redis
            //也就是说 在修改Hbase内容之前，别人又进行了查询，缓存中并没有真的删掉
            //这样step2 插入了新的之后，别的做查询 还是缓存中老的数据

            //解决方案1： 删除redis 插入hbase 再删除一次
            //解决方案2： 维度表都是缓慢变化维 redis中直接修改 不删了
            // 如果更新到redis后,但是没有插入到Hbase，如果此时挂了
            // 别的进程做查询的时候，由于redis有，所以没影响
            // 当程序重启，重新消费binlog,维表的修改仍然会同步到hbase


            //step2. 插入
            preparedStatement.executeUpdate();

        }catch(SQLException e){
            e.printStackTrace();
        }finally{
            if(preparedStatement != null){
                preparedStatement.close();
            }
        }
    }

    private String getUpserSql(String table,JSONObject data){
        Set<String> keySet = data.keySet();  //字段名
        Collection<Object> values = data.values();//字段值


        //sql 这里注意 插入hbase的数据全是varchar类型 所以要加单引号
        return "upsert into" + GmallConfig.HBASE_SCHEMA +
                "." +  table
                + "(" + StringUtils.join(keySet,",") + ")"
                + " values(' " + StringUtils.join(values,"','")
                + "')";
    }
}
