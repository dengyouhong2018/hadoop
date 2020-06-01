package com.hive.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ETLDriver implements Tool {

    private Configuration conf;

    // 程序入口函数
    public static void main(String[] args) throws Exception {

       // args = new String[]{"D:/input/guiliVideo/video/2008/0222","D:/output/guilVideo/"};

        int run = ToolRunner.run(new ETLDriver(), args);
        System.out.println(run);
    }

    @Override
    public int run(String[] args) throws Exception {

        // 1、获取job对象
        Job job = Job.getInstance();

        // 2、封装driver类
        job.setJarByClass(ETLDriver.class);

        // 3、关联mapper类
        job.setMapperClass(ETLMapper.class);

        // 4、mapper输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5、最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reducetask个数为0
        //job.setNumReduceTasks(0);

        // 6、输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7、提交
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
