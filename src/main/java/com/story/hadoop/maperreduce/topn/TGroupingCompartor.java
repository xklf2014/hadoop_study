package com.story.hadoop.maperreduce.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TGroupingCompartor extends WritableComparator {
    public TGroupingCompartor() {
        super(TKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TKey k1 = (TKey)a;
        TKey k2 = (TKey)b;
        //年月，温度（倒叙）
        int c1 = Integer.compare(k1.getYear(), k2.getYear());
        if(c1 == 0){
           return Integer.compare(k1.getMonth(), k2.getMonth());
        }

        return c1;
    }
}
