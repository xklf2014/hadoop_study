package com.story.hadoop.maperreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TReducer extends Reducer<TKey, IntWritable, Text, IntWritable> {

    Text rkey = new Text();
    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(TKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        Iterator<IntWritable> iter = values.iterator();

        int flg = 0;
        int day = 0;
        while (iter.hasNext()) {
            IntWritable val = iter.next(); // -> context.nextKeyValue() ->  对key和value更新值！！！

            if (flg == 0){
                rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                rval.set(key.getTempture());
                context.write(rkey,rval);
                flg++;
                day = key.getDay();
            }

            if (flg != 0 && day != key.getDay()){
                rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
                rval.set(key.getTempture());
                context.write(rkey,rval);
                break;
            }


        }
    }
}
