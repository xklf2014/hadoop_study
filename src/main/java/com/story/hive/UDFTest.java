package com.story.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class UDFTest extends UDF {
    public Text evaluate(final Text text) {
        if (text == null) {
            return null;
        }
        String str = text.toString().substring(0,3)+"***";
        return new Text(str);
    }
}
