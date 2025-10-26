package com.bigdata.task4.weekday;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WeekdayMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text dayType = new Text();
    private Text usageStatus = new Text();
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
            String couponId = fields[2].trim();        // Coupon_id
            String dateReceived = fields[5].trim();    // Date_received
            String date = fields[6].trim();            // Date
            
            // 只统计领取了优惠券的记录
            if ("null".equals(couponId) || "null".equals(dateReceived)) {
                return;
            }
            
            // 解析领取日期，判断是工作日还是周末
            Date receivedDate = dateFormat.parse(dateReceived);
            Calendar cal = Calendar.getInstance();
            cal.setTime(receivedDate);
            
            int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
            
            // Calendar.SUNDAY = 1, Calendar.SATURDAY = 7
            if (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY) {
                dayType.set("周末");
            } else {
                dayType.set("工作日");
            }
            
            // 判断是否使用
            if (!"null".equals(date)) {
                usageStatus.set("used");
            } else {
                usageStatus.set("unused");
            }
            
            context.write(dayType, usageStatus);
            
        } catch (Exception e) {
            // 数据解析异常，跳过
        }
    }
}
