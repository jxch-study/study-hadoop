package org.jxch.study.hadoop.mr.wc.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.jetbrains.annotations.NotNull;

public class WCPartitioner extends Partitioner<DescIntWritable, Text> {

    @Override
    public int getPartition(DescIntWritable descIntWritable, @NotNull Text text, int numPartitions) {
        return text.toString().contains("jxch") ? 0 : 1;
    }

}
