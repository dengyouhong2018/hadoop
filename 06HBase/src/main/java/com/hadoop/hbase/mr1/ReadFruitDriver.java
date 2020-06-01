package com.hadoop.hbase.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ReadFruitDriver extends Configuration implements Tool {

    private Configuration configuration = null;

    @Override
    public int run(String[] strings) throws Exception {

        // 获取任务对象
        Job job = Job.getInstance(configuration);

        // 指定Driver类
        job.setJarByClass(ReadFruitDriver.class);

        // 指定Mapper类
        TableMapReduceUtil.initTableMapperJob("fruit", new Scan(), ReadFruitMapper.class, ImmutableBytesWritable.class, Put.class, job);

        // 指定Reducer类
        TableMapReduceUtil.initTableReducerJob("fruit_mr", ReadFruitReducer.class, job);

        // 提交
        boolean result = job.waitForCompletion(true);

        if(!result){
            throw new IOException("Job running with error");
        }

        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();

        int status = ToolRunner.run(configuration, new ReadFruitDriver(), args);

        System.exit(status);

    }
}
