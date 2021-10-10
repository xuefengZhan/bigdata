package com.fantasy;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {

        //1.获取客户端对象
        URI uri = new URI("hdfs://hadoop102:8020");
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(uri, configuration,"atguigu");

        //2.操作
        fs.mkdirs(new Path("/xiyou/huaguoshan"));

        //3.关闭资源
        fs.close();

    }

}
