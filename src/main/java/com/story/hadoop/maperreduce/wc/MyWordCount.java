package com.story.hadoop.maperreduce.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Arrays;


public class MyWordCount {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);

        //工具类进行解析传入参数
        GenericOptionsParser parser = new GenericOptionsParser(conf,args);
        String[] otherArgs = parser.getRemainingArgs();
        System.out.println(Arrays.toString(otherArgs));
        //让框架知道提交的平台是windos（异构平台）
        conf.set("mapreduce.app-submission.cross-platform","true");
        //conf.set("mapreduce.framework.name","local"); // 单机运行

        Job job = Job.getInstance(conf);


        job.setJar("E:\\java_study\\hadoophdfs\\target\\hadoop-hdfs-1.0.0.1.jar");

        job.setJarByClass(MyWordCount.class);
        job.setJobName("myjob");

        //定义输入文件，可以为多个
        //Path infile = new Path("/data/wc/input");
        Path infile = new Path(otherArgs[0]); //从传入参数中获取输入文件

        //TextInputFormat.setMinInputSplitSize(job,1); 设置切片最小值
        //TextInputFormat.setMaxInputSplitSize(job,Long.MAX_VALUE); 设置切片最大值
        TextInputFormat.addInputPath(job, infile);
        //定义输出文件
        //Path outfile = new Path("/data/wc/output");
        Path outfile = new Path(otherArgs[1]);//从传入参数中获取输出文件
        //如果文件目录存在则进行删除
        if (outfile.getFileSystem(conf).exists(outfile)) {
            outfile.getFileSystem(conf).delete(outfile, true);
        }
        TextOutputFormat.setOutputPath(job, outfile);

        //进行map阶段，将数据切分为K，V，P
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //进行reduce阶段，将同组数据进行计算
        job.setReducerClass(MyReducer.class);
        //job.setNumReduceTasks(2);
        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
