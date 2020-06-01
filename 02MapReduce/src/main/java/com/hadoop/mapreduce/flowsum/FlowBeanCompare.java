package com.hadoop.mapreduce.flowsum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBeanCompare implements WritableComparable<FlowBeanCompare> {

    private long upFlow; //上行流量
    private long downFlow; //下行流量
    private long sumFlow; //总流量（上行流量+下行流量）

    //2  反序列化时，需要反射调用空参构造函数，所以必须有
    public FlowBeanCompare() {
        super();
    }

    public FlowBeanCompare(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    @Override
    public int compareTo(FlowBeanCompare bean) {
        int result;

        // 按照总流量大小，倒序排列
        if (sumFlow > bean.getSumFlow()) {
            result = -1;
        }else if (sumFlow < bean.getSumFlow()) {
            result = 1;
        }else {
            result = 0;
        }
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }
}
