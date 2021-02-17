package com.story.hadoop.maperreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TMapper extends Mapper<LongWritable, Text,TKey, IntWritable> {

    TKey tkey = new TKey();
    IntWritable val = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //2019-6-1 22:22:22	1	39

        String[] strs = StringUtils.split(value.toString(), '\t');
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try{
            Date date = sdf.parse(strs[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);

            tkey.setYear(cal.get(Calendar.YEAR));
            tkey.setMonth(cal.get(Calendar.MONTH)+1);
            tkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
            tkey.setTempture(Integer.parseInt(strs[2]));
            val.set(1);

            context.write(tkey,val);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
