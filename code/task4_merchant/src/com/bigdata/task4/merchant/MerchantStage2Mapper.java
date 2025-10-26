package com.bigdata.task4.merchant;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MerchantStage2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text category = new Text();
    private Text info = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString().trim();
        if (line.isEmpty()) {
            return;
        }
        
        String[] parts = line.split("\t");
        if (parts.length != 2) {
            return;
        }
        
        String[] counts = parts[1].split(",");
        if (counts.length != 2) {
            return;
        }
        
        try {
            int totalCount = Integer.parseInt(counts[0]);
            int usedCount = Integer.parseInt(counts[1]);
            
            // 根据发券量分类
            String cat = categorizeMerchant(totalCount);
            category.set(cat);
            
            // 输出：类别 -> 发券量,使用量
            info.set(totalCount + "," + usedCount);
            context.write(category, info);
            
        } catch (NumberFormatException e) {
            // 跳过异常数据
        }
    }
    
    private String categorizeMerchant(int couponCount) {
        if (couponCount >= 1000) {
            return "高活跃商家(>=1000券)";
        } else if (couponCount >= 500) {
            return "中高活跃商家(500-999券)";
        } else if (couponCount >= 100) {
            return "中等活跃商家(100-499券)";
        } else if (couponCount >= 50) {
            return "中低活跃商家(50-99券)";
        } else {
            return "低活跃商家(<50券)";
        }
    }
}
