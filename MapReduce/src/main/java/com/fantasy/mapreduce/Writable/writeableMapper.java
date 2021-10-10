package com.fantasy.mapreduce.Writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//keyOut: 手机号
//valueOut: 自定义Bean
public class writeableMapper extends Mapper<LongWritable,Text,Text,FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
        //1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String[] split = value.toString().split("\t");
        String phone = split[1];
        String upFlow = split[split.length-3];
        String downFlow = split[split.length-2];

       k.set(phone);
       v.setUpFlow(Integer.parseInt(upFlow));
       v.setDownFlow(Integer.parseInt(downFlow));
       v.setSumFlow(Integer.parseInt(upFlow) + Integer.parseInt(downFlow));

       context.write(k,v);

    }
}
