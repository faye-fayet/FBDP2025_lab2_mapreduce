package com.bigdata.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class MerchantDistanceReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        // key格式：Merchant_id,Distance
        // values：该商家在该距离上的所有User_id（可能有重复）
        
        // 使用HashSet去重，统计不同用户数量
        HashSet<String> uniqueUsers = new HashSet<>();
        
        for (Text value : values) {
            uniqueUsers.add(value.toString());
        }
        
        // 解析key，提取Merchant_id和Distance
        String[] keyParts = key.toString().split(",");
        if (keyParts.length != 2) {
            return;
        }
        
        String merchantId = keyParts[0];
        String distance = keyParts[1];
        
        // 输出格式：Merchant_id TAB Distance=用户数
        outputKey.set(merchantId);
        outputValue.set("Distance=" + distance + ":" + uniqueUsers.size());
        
        context.write(outputKey, outputValue);
    }
}
