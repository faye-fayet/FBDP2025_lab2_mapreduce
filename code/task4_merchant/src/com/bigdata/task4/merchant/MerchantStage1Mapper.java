package com.bigdata.task4.merchant;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MerchantStage1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text merchantId = new Text();
    private Text info = new Text();
    
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
        
        String[] fields = line.split(",");
        
        if (fields.length < 7) {
            return;
        }
        
        try {
            String merchant = fields[1].trim();        // Merchant_id
            String couponId = fields[2].trim();        // Coupon_id
            String dateReceived = fields[5].trim();    // Date_received
            String date = fields[6].trim();            // Date
            
            // 只统计发放了优惠券的记录
            if ("null".equals(couponId) || "null".equals(dateReceived)) {
                return;
            }
            
            merchantId.set(merchant);
            
            // 记录是否使用：1=使用，0=未使用
            if (!"null".equals(date)) {
                info.set("1");
            } else {
                info.set("0");
            }
            
            context.write(merchantId, info);
            
        } catch (Exception e) {
            // 跳过异常数据
        }
    }
}
