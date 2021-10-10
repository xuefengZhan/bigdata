package com.fantasy.mapreduce.Writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class writeableReducer extends Reducer<Text,FlowBean,Text,FlowBean> {
    FlowBean outValue = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values,Context context) throws IOException, InterruptedException {
        int upFlowTotal = 0;
        int downFlowTotal = 0;
        for (FlowBean value : values) {
            upFlowTotal += value.getUpFlow();
            downFlowTotal += value.getDownFlow();
        }

        outValue.setPhone(Long.parseLong(key.toString()));
        outValue.setUpFlow(upFlowTotal);
        outValue.setDownFlow(downFlowTotal);
        outValue.setSumFlow(upFlowTotal + downFlowTotal);

        context.write(key,outValue);
    }
}
