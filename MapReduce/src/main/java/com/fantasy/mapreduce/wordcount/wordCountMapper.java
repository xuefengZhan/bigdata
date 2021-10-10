package com.fantasy.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class wordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text text = new Text();//提到外面 因为map是循环操作，所以避免创建过多对象
    private IntWritable cnt = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // keyIn 是行偏移量
        // valueIn 是一行数据
        String[] words = value.toString().split(" ");

        for (String word : words) {
            text.set(word);
            context.write(text, cnt);
        }
    }
}
