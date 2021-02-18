package com.story.hadoop.hdfs.friendrecommendation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int flg = 0;
        int sum = 0;
        for (IntWritable value : values) {
            if (value.get() == 0){
                flg = 1;
            }
            sum += value.get();
        }

        if (flg == 0){
            rval.set(sum);
            context.write(key,rval);
        }
    }
}
