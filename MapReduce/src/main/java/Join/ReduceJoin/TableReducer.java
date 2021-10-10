package Join.ReduceJoin;

import Join.TableBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean,TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        //todo 1.说明
        // 传过来的以pid作为key，value是Bean
        // 因为order表中有pid会有重复，因此这里用集合存储来自order表的bean对象
        // product表中pid是唯一的，所以一个bean对象即可
        ArrayList<TableBean> orderBeans = new ArrayList<TableBean>();
        TableBean pdBean = new TableBean();
        //todo 2.说明
        // 如果迭代器中是对象类型，迭代器中存储的是对象的地址
        // 如果直接从迭代器中取出元素往集合中添加就是地址，并且会覆盖前面的
        // 所以这里要创建一个临时对象，将迭代器元素指向的对象的数据拷贝过来
        // BeanUtils是hadoop提供的工具类
        for (TableBean value : values) {
            if("order".equals(value.getFlag())){
                TableBean tmpBean = new TableBean();
                //
                try {
                    BeanUtils.copyProperties(tmpBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tmpBean);
            }else{
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        //todo 3.说明
        // 遍历集合，进行join
        for (TableBean orderBean : orderBeans) {
            //将name设置进去
            orderBean.setPname(pdBean.getPname());
            context.write(orderBean,NullWritable.get());
        }

    }
}
