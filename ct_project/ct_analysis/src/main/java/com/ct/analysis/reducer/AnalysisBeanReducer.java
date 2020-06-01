package com.ct.analysis.reducer;

import com.ct.analysis.kv.AnalysisKey;
import com.ct.analysis.kv.AnalysisValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 分析数据的Reducer
 */
public class AnalysisBeanReducer extends Reducer<AnalysisKey, Text, AnalysisKey, AnalysisValue> {

    protected void reduce(AnalysisKey key, Iterable<Text> Text, Context context) throws IOException, InterruptedException {

        int sumCall = 0;
        int sumDuration = 0;


        for (Text value : Text) {
            int duration = Integer.parseInt(value.toString());
            sumDuration += duration;
            sumCall++;
        }

        context.write(key, new AnalysisValue("" + sumCall, "" + sumDuration));

    }
}
