package org.jxch.study.hadoop.mr.wc.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class WCSortMapper extends Mapper<LongWritable, Text, DescIntWritable, Text> {
    @Override
    protected void map(LongWritable key, @NotNull Text value, @NotNull Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        if (words.length == 2) {
            context.write(new DescIntWritable(Integer.parseInt(words[1])), new Text(words[0]));
        }
    }
}
