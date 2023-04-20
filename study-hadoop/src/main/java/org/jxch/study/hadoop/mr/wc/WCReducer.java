package org.jxch.study.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

// 泛型分别是：输入的键值类型; 输出的键值类型
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, @NotNull Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable v : values) {
            count += v.get();
        }
        context.write(key, new IntWritable(count));
    }

}
