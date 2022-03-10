package com.fantasy.mapreduce.MR02_Writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//统计每一个手机号耗费的总上行流量、总下行流量、总流量
public class FlowBean implements Writable {
    private long phone;
    private int upFlow;
    private int downFlow;
    private int sumFlow;


    //todo 1.无参构造
    public FlowBean() {
    }

    public FlowBean(long phone, int upFlow, int downFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    //todo 2.序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(phone);
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
    }

    //todo 2.反序列化方法
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phone = dataInput.readLong();
        upFlow = dataInput.readInt();
        downFlow = dataInput.readInt();
    }

    @Override
    public String toString() {
        return  phone +
                "\t" + upFlow +
                "\t" + downFlow +
                "\t" + sumFlow ;
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
}
