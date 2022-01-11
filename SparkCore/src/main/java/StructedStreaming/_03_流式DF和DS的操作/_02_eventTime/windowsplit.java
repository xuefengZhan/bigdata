package StructedStreaming._03_流式DF和DS的操作._02_eventTime;

import org.apache.spark.sql.sources.In;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class windowsplit {



    private static HashMap<Timestamp,Timestamp> getWindows(double timestamp,double windowDuration,double slideDuration,double startTime){
        double num = Math.ceil(windowDuration/slideDuration);
        System.out.println("窗口个数：" + num);
        double windowId = Math.ceil((timestamp - startTime) / slideDuration);
        System.out.println("窗口id：" + windowId);



        HashMap<Double, Double> result = new HashMap<>();
        HashMap<Timestamp,Timestamp> result2 = new HashMap<>();


        for(int i = 0;i<num;i++){
            double windowStart = windowId * slideDuration + (i - num) * slideDuration + startTime;
            double windowEnd = windowStart + windowDuration;

            Timestamp start = new Timestamp((long) windowStart);
            System.out.println("窗口起始时间：" + start);
            Timestamp end = new Timestamp((long) windowEnd);
            System.out.println("窗口结束时间：" + end);

            result.put(windowStart,windowEnd);
            result2.put( start,end);
        }

        return result2;
    }

    //1634551127649  2021-10-18 17:58:47
    public static void main(String[] args) {
        double w = 10;
        double s = 3;
        System.out.println(w/s);
        System.out.println(Math.ceil(w/s));

        System.out.println(System.currentTimeMillis());
        //System.currentTimeMillis()
        HashMap<Timestamp,Timestamp> windows = getWindows(1634551127649.0, 10 * 60 * 1000, 3 * 60 * 1000, 0);

        for (Map.Entry<Timestamp,Timestamp> doubleDoubleEntry : windows.entrySet()) {
            System.out.println(doubleDoubleEntry);
        }
    }
}
