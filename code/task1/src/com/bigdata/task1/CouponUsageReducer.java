package com.bigdata.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CouponUsageReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        int negativeCount = 0;  // 负样本数（领取未使用）
        int normalCount = 0;    // 普通消费数（未领取直接消费）
        int positiveCount = 0;  // 正样本数（领取并使用）
        
        // 累加各类型的数量
        for (Text value : values) {
            String[] counts = value.toString().split(",");
            negativeCount += Integer.parseInt(counts[0]);
            normalCount += Integer.parseInt(counts[1]);
            positiveCount += Integer.parseInt(counts[2]);
        }
        
        // 输出格式: Merchant_id TAB 负样本数 TAB 普通消费数 TAB 正样本数
        result.set(negativeCount + "\t" + normalCount + "\t" + positiveCount);
        context.write(key, result);
    }
}
