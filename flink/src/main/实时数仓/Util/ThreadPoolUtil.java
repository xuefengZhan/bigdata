package Util;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


//单例模式
public class ThreadPoolUtil {

    /**
     * ThreadPoolExecutor四个参数：
     *  corePoolSize : 核心线程数  也就是说池子中至少保留4个
     *  maxmumPoolSize : 池子最大线程个数
     *  KeepAliceTime : 多于4个的额外线程 没事情干的时候，到这个时间阈值就会被释放
     *  workQueue: 任务等待队列，当4个都被占用了，任务进队列等待；当等待队列满了才会开辟新的线程
     */
    static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil(){}

    public static ThreadPoolExecutor getThreadPool(){
        if(threadPoolExecutor == null){
           synchronized (ThreadPoolUtil.class){
               if(threadPoolExecutor == null){
                   threadPoolExecutor = new ThreadPoolExecutor(8
                           , 16, 1, TimeUnit.MINUTES,
                           new LinkedBlockingDeque<>());
               }
           }
        }
        return threadPoolExecutor;
    }
}
