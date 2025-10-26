package com.bigdata.task4.merchant;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MerchantStage2Reducer extends Reducer<Text, Text, Text, Text> {
    
    private Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        long totalCoupons = 0;
        long usedCoupons = 0;
        int merchantCount = 0;
        
        for (Text value : values) {
            String[] counts = value.toString().split(",");
            if (counts.length == 2) {
                totalCoupons += Long.parseLong(counts[0]);
                usedCoupons += Long.parseLong(counts[1]);
                merchantCount++;
            }
        }
        
        double usageRate = totalCoupons > 0 ? (double) usedCoupons / totalCoupons * 100 : 0;
        double avgCouponsPerMerchant = merchantCount > 0 ? (double) totalCoupons / merchantCount : 0;
        
        result.set(String.format("商家数:%d, 总发券:%d, 总使用:%d, 使用率:%.2f%%, 平均发券:%.1f", 
                   merchantCount, totalCoupons, usedCoupons, usageRate, avgCouponsPerMerchant));
        
        context.write(key, result);
    }
}
