package com.story.hadoop.hdfs.friendrecommendation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FriendRecommendation {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.app-submission.cross-platform","true");
        //conf.set("mapreduce.framework.name","local"); // 单机运行

        GenericOptionsParser parser = new GenericOptionsParser(conf,args);
        String[] remainingArgs = parser.getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJarByClass(FriendRecommendation.class);
        job.setJobName("FriendRecommendation");

        job.setJar("E:\\java_study\\hadoophdfs\\target\\hadoop-hdfs-1.0.0.1.jar");
        //设置输入路径
        for (int i = 0; i < remainingArgs.length-1; i++) {
            TextInputFormat.addInputPath(job,new Path(remainingArgs[i]));
        }

        //设置输入路径
        Path outPath = new Path(remainingArgs[remainingArgs.length-1]);
        if (outPath.getFileSystem(conf).exists(outPath)){
            outPath.getFileSystem(conf).delete(outPath,true);
        }

        TextOutputFormat.setOutputPath(job,outPath);

        //map task
        job.setMapperClass(FMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //reduce
        job.setReducerClass(FReducer.class);

        job.waitForCompletion(true);
    }
}
