package Function;

import Bean.OrderWide;
import Util.DimUtil;
import Util.ThreadPoolUtil;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 * @param <T>  输入和输出泛型
 */
public abstract  class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimAsyncJoinFunction<T> {
    private Connection connection;
    ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    //open中初始化Hbase连接
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        //利用线程池提交一个任务
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                //查询维度信息  表名 + id查
                String id = getKey(input);
                try {
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                    //补充维度信息
                    if(dimInfo!=null){
                        join(input,dimInfo);
                    }

                    //将数据输出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException | ParseException e) {
                    e.printStackTrace();
                }

            }
        });
    }


    /**
     * 超时  异步调用没有响应
     * @param input
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut:" + input);
    }
}
