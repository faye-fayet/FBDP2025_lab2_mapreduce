package com.bigdata.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CouponUsageMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text merchantId = new Text();
    private Text valueOut = new Text();
    private boolean isOnlineData = false;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 通过文件名判断是线上还是线下数据
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        isOnlineData = fileName.contains("online");
    }
    
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
        
        try {
            if (isOnlineData) {
                // 线上数据：User_id,Merchant_id,Action,Coupon_id,Discount_rate,Date_received,Date
                if (fields.length < 7) {
                    return;
                }
                
                String merchant = fields[1].trim(); // Merchant_id
                String action = fields[2].trim();   // Action
                String couponId = fields[3].trim(); // Coupon_id
                String dateReceived = fields[5].trim(); // Date_received
                String date = fields[6].trim(); // Date
                
                merchantId.set(merchant);
                
                // Action: 0=点击, 1=购买, 2=领取优惠券
                if ("1".equals(action)) {
                    // 购买行为
                    if ("null".equals(couponId) && "null".equals(dateReceived)) {
                        // 普通消费（未领取优惠券直接消费）
                        valueOut.set("0,1,0");
                        context.write(merchantId, valueOut);
                    } else if (!"null".equals(couponId) && !"null".equals(dateReceived) && !"null".equals(date)) {
                        // 领取优惠券并使用
                        valueOut.set("0,0,1");
                        context.write(merchantId, valueOut);
                    }
                } else if ("2".equals(action)) {
                    // 领取优惠券行为
                    if (!"null".equals(couponId) && !"null".equals(dateReceived)) {
                        if ("null".equals(date)) {
                            // 领取但未使用
                            valueOut.set("1,0,0");
                            context.write(merchantId, valueOut);
                        }
                    }
                }
                
            } else {
                // 线下数据：User_id,Merchant_id,Coupon_id,Discount_rate,Distance,Date_received,Date
                if (fields.length < 7) {
                    return;
                }
                
                String merchant = fields[1].trim(); // Merchant_id
                String couponId = fields[2].trim(); // Coupon_id
                String dateReceived = fields[5].trim(); // Date_received
                String date = fields[6].trim(); // Date
                
                merchantId.set(merchant);
                
                // 判断消费行为类型
                if (!"null".equals(couponId) && !"null".equals(dateReceived)) {
                    if (!"null".equals(date)) {
                        // 领取优惠券并使用（正样本）
                        valueOut.set("0,0,1");
                    } else {
                        // 领取优惠券未使用（负样本）
                        valueOut.set("1,0,0");
                    }
                    context.write(merchantId, valueOut);
                } else if ("null".equals(couponId) && "null".equals(dateReceived) && !"null".equals(date)) {
                    // 未领取优惠券直接消费（普通消费）
                    valueOut.set("0,1,0");
                    context.write(merchantId, valueOut);
                }
            }
            
        } catch (Exception e) {
            // 数据解析异常，跳过该行
            System.err.println("Error parsing line: " + line);
            e.printStackTrace();
        }
    }
}