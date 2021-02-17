package com.story.hadoop.maperreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartitioner extends Partitioner<TKey, IntWritable> {
    @Override
    public int getPartition(TKey tKey, IntWritable intWritable, int numPartitions) {
        return tKey.getYear() % numPartitions; //可能出现数据倾斜
    }
}
