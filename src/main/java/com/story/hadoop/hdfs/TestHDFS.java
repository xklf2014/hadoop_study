package com.story.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class TestHDFS {

    public Configuration conf = null;
    public FileSystem fs = null;

    @Before
    public void conn() throws Exception {
        conf = new Configuration(true);
        //环境变量HADOOP_USER_NAME
        //fs = FileSystem.get(conf);
        fs = FileSystem.get(URI.create("hdfs://mycluster/"),conf,"god");

    }


    @Test
    public void mkdir() throws Exception {

        Path dir = new Path("/test");
        if(fs.exists(dir)){
            fs.delete(dir,true);
        }
        fs.mkdirs(dir);
    }

    @Test
    public void upload() throws Exception {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(new File("src/main/resource/pic.jpg")));

        Path  outfile = new Path("/test/picture.jpg");
        FSDataOutputStream out = fs.create(outfile);

        IOUtils.copyBytes(in,out,conf,true);

    }

    @Test
    public void download() throws Exception {
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File("src/main/resource/downpic.jpg")));
        Path infile = new Path("/test/picture.jpg");
        FSDataInputStream in = fs.open(infile, 4096);
        IOUtils.copyBytes(in,out,conf,true);

    }

    @Test
    public void blockSize() throws Exception {
        Path path = new Path("/user/god/dataTopN.txt");
        FileStatus fss = fs.getFileStatus(path);
        BlockLocation[] blks = fs.getFileBlockLocations(fss, 0, fss.getLen());
        for (BlockLocation blk : blks) {
            System.out.println(blk);
        }
       /* 0,1048576,zknode02,zknode04
        1048576,840319,zknode02,zknode04*/

        FSDataInputStream in = fs.open(path);
        in.seek(1048576);//切换道第二个block的起始位置

        //计算向数据移动，期望的是分治，只读取自己关心的数据，同时具备距离的概念
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());
        System.out.println((char)in.readByte());

    }

    @After
    public void close() throws Exception {
        fs.close();

    }
}
