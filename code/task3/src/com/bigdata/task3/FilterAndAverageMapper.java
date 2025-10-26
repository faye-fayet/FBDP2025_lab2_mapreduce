package com.bigdata.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class FilterAndAverageMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private long totalUsageCount = 0;
    private long threshold = 0;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 读取第一阶段的输出，计算总使用次数
        Configuration conf = context.getConfiguration();
        String tempPath = conf.get("temp.output.path");
        
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(tempPath + "/part-r-00000");
        
        BufferedReader br = new BufferedReader(
            new InputStreamReader(fs.open(outputPath))
        );
        
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                String[] countAndTotal = parts[1].split(",");
                if (countAndTotal.length == 2) {
                    totalUsageCount += Long.parseLong(countAndTotal[0]);
                }
            }
        }
        br.close();
        
        // 计算1%阈值
        threshold = (long) Math.ceil(totalUsageCount * 0.01);
        
        System.out.println("Total usage count: " + totalUsageCount);
        System.out.println("Threshold (1%): " + threshold);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] parts = line.split("\t");
        
        if (parts.length != 2) {
            return;
        }
        
        String couponId = parts[0];
        String[] countAndTotal = parts[1].split(",");
        
        if (countAndTotal.length != 2) {
            return;
        }
        
        try {
            long count = Long.parseLong(countAndTotal[0]);
            long totalDays = Long.parseLong(countAndTotal[1]);
            
            // 只保留使用次数大于阈值的优惠券
            if (count > threshold) {
                double avgDays = (double) totalDays / count;
                
                // 输出：平均间隔 -> Coupon_id,平均间隔
                // 使用平均间隔作为key，便于排序
                Text outputKey = new Text(String.format("%.2f", avgDays));
                Text outputValue = new Text(couponId + "," + String.format("%.2f", avgDays));
                
                context.write(outputKey, outputValue);
            }
        } catch (NumberFormatException e) {
            // 解析错误，跳过
        }
    }
}
