package com.bigdata.task4.discount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DiscountRateMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text discountCategory = new Text();
    private Text usageStatus = new Text();
    
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
            String discountRate = fields[3].trim();    // Discount_rate
            String dateReceived = fields[5].trim();    // Date_received
            String date = fields[6].trim();            // Date
            
            // 只统计领取了优惠券的记录
            if ("null".equals(couponId) || "null".equals(dateReceived)) {
                return;
            }
            
            // 解析折扣率并分类
            String category = parseDiscountCategory(discountRate);
            if (category == null) {
                return;
            }
            
            discountCategory.set(category);
            
            // 判断是否使用
            if (!"null".equals(date)) {
                usageStatus.set("used");  // 已使用
            } else {
                usageStatus.set("unused");  // 未使用
            }
            
            context.write(discountCategory, usageStatus);
            
        } catch (Exception e) {
            // 数据解析异常，跳过
        }
    }
    
    /**
     * 解析折扣率并分类
     * 折扣率格式：x:y（满x减y）或 0.9（直接折扣）
     */
    private String parseDiscountCategory(String discountRate) {
        if ("null".equals(discountRate) || discountRate.isEmpty()) {
            return null;
        }
        
        try {
            if (discountRate.contains(":")) {
                // 满减类型：x:y
                String[] parts = discountRate.split(":");
                if (parts.length != 2) {
                    return null;
                }
                
                double full = Double.parseDouble(parts[0]);
                double minus = Double.parseDouble(parts[1]);
                
                // 计算实际折扣率
                double actualRate = (full - minus) / full;
                
                return categorizeRate(actualRate);
                
            } else {
                // 直接折扣：0.9
                double rate = Double.parseDouble(discountRate);
                return categorizeRate(rate);
            }
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    /**
     * 将折扣率分类
     */
    private String categorizeRate(double rate) {
        if (rate >= 0.9) {
            return "0.9-1.0_低折扣";
        } else if (rate >= 0.8) {
            return "0.8-0.9_中低折扣";
        } else if (rate >= 0.7) {
            return "0.7-0.8_中等折扣";
        } else if (rate >= 0.5) {
            return "0.5-0.7_中高折扣";
        } else {
            return "0.0-0.5_高折扣";
        }
    }
}
