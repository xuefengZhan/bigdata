package Util;

import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

//用于查询维表指定主键的记录
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {

//        //todo 方案1 直接查询Hbase
//        String sql = "select * from "
//                + GmallConfig.HBASE_SCHEMA + "." + tableName
//                + "where id = '" + id + "'";
//
//        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, sql, JSONObject.class, true);
//
//        return jsonObjects.get(0);

        //todo 方案2 旁路缓存优化
        //\查询redis 命中直接返回，没命中 查询Hbase 将结果放进缓存
        //Jedis jedis = RedisUtil.getJedis();
        Jedis jedis = null;
        String redisKey = "DIM" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if(dimInfoJsonStr != null){
            //情况1 查到了
            //重置TTL
            jedis.expire(redisKey,24 * 60 * 60);
            jedis.close();
            return JSONObject.parseObject(dimInfoJsonStr);
        }
        // 情况2  没有查到缓存 则查Hbase
        String sql = "select * from "
                + GmallConfig.HBASE_SCHEMA + "." + tableName
                + "where id = '" + id + "'";

        List<JSONObject> jsonObjects = JdbcUtil.queryList(connection, sql, JSONObject.class, true);

        //返回数据之前将数据写到redis
        JSONObject jsonObject = jsonObjects.get(0);
        jedis.set(redisKey, jsonObject.toJSONString());
        jedis.expire(redisKey,24 * 60 * 60);
        jedis.close();

        return jsonObject;


    }

    //删除key的方法
    public static void delRedisDimInfo(String tableName,String id){
       // Jedis jedis = RedisUtil.getJedis();
        Jedis jedis = null;
        String redisKey = "DIM" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }


    //使用演示
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        JSONObject user_info = getDimInfo(connection, "DIM_USER_INFO", "143");

        System.out.println(user_info);
    }
}
