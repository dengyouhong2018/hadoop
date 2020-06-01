package com.hadoop.mapreduce.outputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper extends Mapper<IntWritable, Text, Text, NullWritable> {

    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}
