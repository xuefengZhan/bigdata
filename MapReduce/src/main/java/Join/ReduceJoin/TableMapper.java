package Join.ReduceJoin;

import Join.TableBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text,Text, TableBean> {

    private String fileName ;
    private Text outK;
    private TableBean outV;

    //Called once at the beginning of the task.


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //todo 通过切片获取数据源文件路径
        FileSplit split = (FileSplit) context.getInputSplit(); //只有fileSplit才有getPath方法
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        //判断来自哪个文件
        if(fileName.contains(("order"))){
            //订单表
            //id	pid	amount
            //1001	01	1
            String[] split = line.split("\t");

            outK.set(split[1]);

            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            //没有也要给  给默认值
            outV.setPname("");
            outV.setFlag("order");
        }else{
            //商品信息表
            //pid	pname
            //01	小米
            String[] split = line.split("\t");

            outK.set(split[0]);

            outV.setId("");
            outV.setPid(split[0]);
            //没有也要给  给默认值
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("product");
        }
        context.write(outK,outV);
    }
}
