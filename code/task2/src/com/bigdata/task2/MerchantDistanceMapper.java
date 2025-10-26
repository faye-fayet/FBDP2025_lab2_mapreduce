package com.bigdata.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

public class MerchantDistanceMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        // 跳过表头
        if (key.get() == 0) {
            return;
        }
        
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }
        
        // 线下数据格式：User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
        String[] fields = line.split(",");
        
        if (fields.length < 7) {
            return;
        }
        
        try {
            String userId = fields[0].trim();      // User_id
            String merchantId = fields[1].trim();  // Merchant_id
            String distance = fields[4].trim();    // Distance
            
            // 处理Distance为null的情况
            if ("null".equals(distance) || distance.isEmpty()) {
                distance = "null";
            }
            
            // 输出格式：Key = "Merchant_id,Distance", Value = "User_id"
            // 这样可以在Reducer中对每个商家的每个距离进行去重统计
            outputKey.set(merchantId + "," + distance);
            outputValue.set(userId);
            
            context.write(outputKey, outputValue);
            
        } catch (Exception e) {
            // 数据解析异常，跳过
            System.err.println("Error parsing line: " + line);
        }
    }
}
