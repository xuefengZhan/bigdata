package com.fantasy.mapreduce.writableComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ComparableMapper extends Mapper<LongWritable, Text,FlowBean,Text> {

    FlowBean outK = new FlowBean();
    Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phone = split[0];
        int downFlow = Integer.parseInt(split[1]);
        int upFlow = Integer.parseInt(split[2]);
        int sumFlow = Integer.parseInt(split[3]);


        outK.setDownFlow(downFlow);
        outK.setUpFlow(upFlow);
        outK.setSumFlow(sumFlow);

        outV.set(phone);
        context.write(outK,outV);

    }
}
