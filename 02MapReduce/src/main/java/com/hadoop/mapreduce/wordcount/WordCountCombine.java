package com.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义Combiner实现步骤（对每一个maptask的输出局部汇总）
 *  1、自定义一个Combiner继承Reducer，重写Reduce方法（combiner组件的父类就是Reducer）
 *  2、在Job驱动类中设置
 */
public class WordCountCombine extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable value = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        // 1、汇总操作
        for (IntWritable value : values) {
            sum += value.get();
        }

        value.set(sum);

        // 2、写出
        context.write(key, value);

    }
}
