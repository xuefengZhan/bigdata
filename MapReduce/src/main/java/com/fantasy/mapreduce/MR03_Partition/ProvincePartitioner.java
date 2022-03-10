package com.fantasy.mapreduce.MR03_Partition;

import com.fantasy.mapreduce.MR02_Writable.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


//分区器的k和v是Mapper的输出
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        // 1 获取电话号码的前三位
        String preNum = key.toString().substring(0, 3);

        int partition = 4;

        // 2 判断是哪个省
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }

        return partition;
    }
}