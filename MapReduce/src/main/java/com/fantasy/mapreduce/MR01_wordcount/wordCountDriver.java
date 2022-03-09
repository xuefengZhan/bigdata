package com.fantasy.mapreduce.MR01_wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class wordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //1.获取配置信息 和 Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.关联本Driver程序的jar
        job.setJarByClass(wordCountDriver.class);

        //3.关联Mapper类 和Reducer类
        job.setMapperClass(wordCountMapper.class);
        job.setReducerClass(wordCountReducer.class);

        //4.设置Mapper和Reducer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5.设置输入输出文件路径
        FileInputFormat.setInputPaths(job, new Path("E:\\work\\bigdata\\MapReduce\\src\\main\\resources\\wordcount.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Hadoop\\MapReduce"));


        //6.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0 : 1);
    }
}
