package org.jxch.study.hadoop.mr.wc.sort;

import org.apache.hadoop.io.IntWritable;

public class DescIntWritable extends IntWritable {

    public DescIntWritable() {
    }

    public DescIntWritable(int value) {
        super(value);
    }

    @Override
    public int compareTo(IntWritable o) {
        return -super.compareTo(o);
    }
}
