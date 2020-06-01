package com.hadoop.mapreduce.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<FlowBean, Text> {

    @Override
    public int getPartition(FlowBean key, Text value, int numPartitions) {

        // 1 获取手机号码前三位
        String preNum = value.toString().substring(0, 3);

        int partition;

        // 2 根据手机号归属地设置分区
        if ("136".equals(preNum)) {
            partition = 0;
        } else if ("137".equals(preNum)) {
            partition = 1;
        } else if ("138".equals(preNum)) {
            partition = 2;
        } else if ("139".equals(preNum)) {
            partition = 3;
        } else {
            partition = 4;
        }

        return partition;
    }
}
