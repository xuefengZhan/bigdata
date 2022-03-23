package No02_source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;


/**
 *  自定义非并行的sourceFunction
 *
 *  文件发生了改变就会往下发
 */
public class FileCountryDictSourceFunction implements SourceFunction<String> {

    private Boolean isCancel = true;
    private final Integer interval = 10000;
    private String md5 = null;

    /**
     * 此方法不会由flink主动调用，得自己定义什么时候调用什么时候退出
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Path path = new Path("/users/111.txt");
        FileSystem fs = FileSystem.get(new Configuration());

        while(isCancel){
            if(!fs.exists(path)){
                Thread.sleep(interval);
                continue;
            }

            //读取hdfs文件的校验码  这里给的参数需是文件元数据
            FileChecksum fileChecksum = fs.getFileChecksum(path);
            String md5Str = fileChecksum.toString();
            String currentMD5 = md5Str.substring(md5Str.indexOf(":") + 1);
            // 当md5不相同了，说明文件数据发生了改变
            if(!currentMD5.equals(md5) ){
                FSDataInputStream open = fs.open(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(open));

                String line = reader.readLine();

                while(line!=null){
                    ctx.collect(line);//发出去
                    line = reader.readLine();
                }
                reader.close();
                md5 = currentMD5;
            }

            Thread.sleep(interval);

        }

    }


    /**
     *  Cancel job的时候会调用这个方法，source退出 下游也会退出
     */
    @Override
    public void cancel() {
        isCancel = false;
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceDS = env.addSource(new FileCountryDictSourceFunction());
        //非并行source 设置并行度 会报错
        sourceDS.setParallelism(2);
        sourceDS.print();
        env.execute();
    }
}
