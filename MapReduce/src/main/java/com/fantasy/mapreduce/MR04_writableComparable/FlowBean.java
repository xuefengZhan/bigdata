package com.fantasy.mapreduce.MR04_writableComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {

    private long phone;
    private int upFlow;
    private int downFlow;
    private int sumFlow;



    public FlowBean() {
    }

    public long getPhone() {
        return phone;
    }

    public void setPhone(long phone) {
        this.phone = phone;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(int sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        int result = o.getSumFlow() -  this.getSumFlow();

        //二次排序
        if(result == 0){
            return this.getUpFlow() - o.getUpFlow();
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readInt();
        downFlow = in.readInt();
        sumFlow = in.readInt();
    }

    @Override
    public String toString() {
       return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
