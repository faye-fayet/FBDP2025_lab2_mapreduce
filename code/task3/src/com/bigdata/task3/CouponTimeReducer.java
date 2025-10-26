package com.bigdata.task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CouponTimeReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text outputValue = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        int count = 0;           // 使用次数
        long totalDays = 0;      // 总间隔天数
        
        // 累加所有使用记录的间隔天数
        for (Text value : values) {
            try {
                long days = Long.parseLong(value.toString());
                totalDays += days;
                count++;
            } catch (NumberFormatException e) {
                // 数据格式错误，跳过
                continue;
            }
        }
        
        if (count > 0) {
            // 输出：Coupon_id -> 使用次数,总间隔天数
            outputValue.set(count + "," + totalDays);
            context.write(key, outputValue);
        }
    }
}
