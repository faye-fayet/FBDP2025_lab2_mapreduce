package com.bigdata.task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterAndAverageReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        // values中的每个元素格式：Coupon_id,平均间隔
        for (Text value : values) {
            String[] parts = value.toString().split(",");
            if (parts.length == 2) {
                String couponId = parts[0];
                String avgDays = parts[1];
                
                // 输出：Coupon_id TAB 平均间隔天数
                context.write(new Text(couponId), new Text(avgDays));
            }
        }
    }
}
