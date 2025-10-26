package com.bigdata.task4.merchant;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MerchantStage1Reducer extends Reducer<Text, Text, Text, Text> {
    
    private Text outputValue = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        int totalCount = 0;
        int usedCount = 0;
        
        for (Text value : values) {
            totalCount++;
            if ("1".equals(value.toString())) {
                usedCount++;
            }
        }
        
        // 输出：Merchant_id -> 发券量,使用量
        outputValue.set(totalCount + "," + usedCount);
        context.write(key, outputValue);
    }
}
