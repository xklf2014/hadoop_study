package com.story.hadoop.hdfs.friendrecommendation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    Text mkey = new Text();
    IntWritable mval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //张三 李四 王五 赵六
        //李四 王五 刘七
        String[] split = StringUtils.split(value.toString(), ' ');
        for (int i = 1; i < split.length; i++) {
            mkey.set(getFriend(split[0],split[i]));
            mval.set(0); // 直接好友
            context.write(mkey,mval);
            for (int j = i+1; j < split.length; j++) {
                mkey.set(getFriend(split[i],split[j]));
                mval.set(1); //间接好友
                context.write(mkey,mval);
            }
        }
    }

    //好友排序
    public static String getFriend(String f1,String f2){
        return f1.compareTo(f2)>0 ? (f1 + "-" + f2) : (f2 + "-" + f1);
    }
}



