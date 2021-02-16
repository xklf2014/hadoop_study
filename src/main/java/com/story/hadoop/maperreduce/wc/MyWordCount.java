package com.story.hadoop.maperreduce.wc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MyWordCount {


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        job.setJarByClass(MyWordCount.class);
        job.setJobName("myjob");

        //定义输入文件，可以为多个
        Path infile = new Path("/data/wc/input");
        TextInputFormat.addInputPath(job, infile);
        //定义输出文件
        Path outfile = new Path("/data/wc/output");
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

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);
    }
}
