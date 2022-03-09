package com.fantasy.mapreduce.MR05_partitionSort;

import com.fantasy.mapreduce.MR02_Writable.FlowBean;
import com.fantasy.mapreduce.MR02_Writable.writeableMapper;
import com.fantasy.mapreduce.MR02_Writable.writeableReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class partitionSortDriver {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(com.fantasy.mapreduce.MR02_Writable.writeableDriver.class);
        job.setMapperClass(writeableMapper.class);
        job.setReducerClass(writeableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8 指定自定义数据分区
        job.setPartitionerClass(partitioner.class);
        // 9 同时指定相应数量的reduce task
        job.setNumReduceTasks(5);


        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
