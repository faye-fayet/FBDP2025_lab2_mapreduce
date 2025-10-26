package com.bigdata.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CouponTimeMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text couponId = new Text();
    private Text outputValue = new Text();
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    
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
            String coupon = fields[2].trim();        // Coupon_id
            String dateReceived = fields[5].trim();  // Date_received
            String date = fields[6].trim();          // Date
            
            // 只统计：Coupon_id不为null，且优惠券被使用（Date不为null）
            if ("null".equals(coupon) || coupon.isEmpty() ||
                "null".equals(dateReceived) || dateReceived.isEmpty() ||
                "null".equals(date) || date.isEmpty()) {
                return;
            }
            
            // 计算日期间隔（天数）
            Date receivedDate = dateFormat.parse(dateReceived);
            Date usedDate = dateFormat.parse(date);
            
            long diffInMillis = usedDate.getTime() - receivedDate.getTime();
            long diffInDays = diffInMillis / (1000 * 60 * 60 * 24);
            
            // 输出：Coupon_id -> 使用间隔天数
            couponId.set(coupon);
            outputValue.set(String.valueOf(diffInDays));
            
            context.write(couponId, outputValue);
            
        } catch (ParseException e) {
            // 日期解析失败，跳过
            System.err.println("Date parse error: " + line);
        } catch (Exception e) {
            // 其他异常，跳过
            System.err.println("Error parsing line: " + line);
        }
    }
}
