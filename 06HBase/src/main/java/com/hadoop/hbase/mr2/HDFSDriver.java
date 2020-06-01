package com.hadoop.hbase.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class HDFSDriver extends Configuration implements Tool {

    private Configuration configuration = null;

    @Override
    public int run(String[] args) throws Exception {

        // 获取任务对象
        Job job = Job.getInstance(configuration);

        // 指定Driver类
        job.setJarByClass(HDFSDriver.class);

        // 指定Mapper类
        job.setMapperClass(ReadFruitFromHDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        // 指定Reducer类
        TableMapReduceUtil.initTableReducerJob("fruit_mr2", WriteFruitMRFromHBaseReducer.class, job);

        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 提交
        boolean result = job.waitForCompletion(true);
        if (!result) {
            throw new IOException("Job running with error");
        }

        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();

        int status = ToolRunner.run(configuration, new HDFSDriver(), args);

        System.exit(status);
    }
}
