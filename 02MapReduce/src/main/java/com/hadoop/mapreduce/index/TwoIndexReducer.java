package com.hadoop.mapreduce.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();

        // 1 拼接
        for (Text value : values) {

            sb.append(value.toString().replaceAll("\t", "-->") + "\t");

        }

        v.set(sb.toString());

        // 2 写出
        context.write(key, v);

    }
}
