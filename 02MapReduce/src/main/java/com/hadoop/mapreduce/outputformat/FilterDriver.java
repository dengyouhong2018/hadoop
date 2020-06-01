package com.hadoop.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[]{"D:/input/inputoutputformat3", "D:/output/outputforamt3"};

        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(FilterDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        // 4 设置mpa输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 要将自定义的输出格式设置到job中
        job.setOutputFormatClass(FilterOutputFormat.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录

        // 7 提交
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);

    }
}
