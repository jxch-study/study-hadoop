package org.jxch.study.hadoop.mr.wc.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class WCSortReducer extends Reducer<DescIntWritable, Text, Text, IntWritable> {
    @Override
    protected void reduce(DescIntWritable key, @NotNull Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text word : values) {
            context.write(word, key);
        }
    }
}
