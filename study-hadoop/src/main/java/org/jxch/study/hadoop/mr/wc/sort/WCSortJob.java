package org.jxch.study.hadoop.mr.wc.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jxch.study.hadoop.mr.wc.WCMapper;
import org.jxch.study.hadoop.mr.wc.WCReducer;

import java.io.IOException;

public class WCSortJob {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(WCSortJob.class);

        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);

//        Mapper 输出键值类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

//        Reducer 输出键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("/wc/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wc/output"));

//        等待执行完并检查是否执行成功
        if (job.waitForCompletion(true)) {
            Job sortJob = Job.getInstance(new Configuration());

            sortJob.setJarByClass(WCSortJob.class);
            sortJob.setMapperClass(WCSortMapper.class);
            sortJob.setReducerClass(WCSortReducer.class);
            sortJob.setMapOutputKeyClass(DescIntWritable.class);
            sortJob.setMapOutputValueClass(Text.class);
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(IntWritable.class);
            sortJob.setInputFormatClass(TextInputFormat.class);
            sortJob.setOutputFormatClass(TextOutputFormat.class);
            sortJob.setPartitionerClass(WCPartitioner.class);
            sortJob.setNumReduceTasks(2);

            FileInputFormat.setInputPaths(sortJob, new Path("/wc/output"));
            FileOutputFormat.setOutputPath(sortJob, new Path("/wc/output_sort"));

            System.exit(sortJob.waitForCompletion(true)? 0:-1);
        }
    }
}
