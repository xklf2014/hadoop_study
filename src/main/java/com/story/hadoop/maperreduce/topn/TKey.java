package com.story.hadoop.maperreduce.topn;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//自定义类型需要实现接口：序列化，反序列化，比较器
public class TKey implements WritableComparable<TKey> {
    private int year;
    private int month;
    private int day;
    private int tempture;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getTempture() {
        return tempture;
    }

    public void setTempture(int tempture) {
        this.tempture = tempture;
    }


    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(tempture);
    }

    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.tempture = in.readInt();
    }

    public int compareTo(TKey o) {
        int c1 = Integer.compare(this.year, o.getYear());

        if(c1 == 0){
            int c2 = Integer.compare(this.month, o.getMonth());
            if(c2 == 0){
                return   Integer.compare(this.day,o.getDay());
            }
            return  c2;
        }

        return c1;

    }
}
