package com.story.hadoop.maperreduce.topn;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class TSortCompartor extends WritableComparator {
    public TSortCompartor() {
        super(TKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TKey k1 = (TKey)a;
        TKey k2 = (TKey)b;
        //年月，温度（倒叙）
        int c1 = Integer.compare(k1.getYear(), k2.getYear());
        if(c1 == 0){
            int c2 = Integer.compare(k1.getMonth(), k2.getMonth());
            if (c2 == 0){
                return  Integer.compare(k2.getTempture(),k1.getTempture());
            }else{
                return c2;
            }
        }

        return c1;
    }
}
