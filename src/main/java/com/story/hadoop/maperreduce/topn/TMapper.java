package com.story.hadoop.maperreduce.topn;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class TMapper extends Mapper<LongWritable, Text, TKey, IntWritable> {

    TKey tkey = new TKey();
    IntWritable val = new IntWritable();

    public HashMap<String, String> dict = new HashMap<String, String>();


    @Override
    /**
     * 如果数据过大，不适合cache
     * 可以做2次map+reduce
     *  cache仅集群运行
     * */
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        for (URI cacheFile : cacheFiles) {
            Path path = new Path(cacheFile.getPath());

            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(path.getName())));

            String line = bufferedReader.readLine();
            while (line != null) {
                //dict   1  上海
                //       2  北京
                //       3  深圳
                String[] split = line.split("\t");
                dict.put(split[0], split[1]);
                line = bufferedReader.readLine();

            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //2019-6-1 22:22:22	1	39

        String[] strs = StringUtils.split(value.toString(), '\t');
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = sdf.parse(strs[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);

            tkey.setYear(cal.get(Calendar.YEAR));
            tkey.setMonth(cal.get(Calendar.MONTH) + 1);
            tkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
            tkey.setTempture(Integer.parseInt(strs[2]));
            tkey.setLocation(dict.get(strs[1]));
            val.set(1);

            context.write(tkey, val);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
