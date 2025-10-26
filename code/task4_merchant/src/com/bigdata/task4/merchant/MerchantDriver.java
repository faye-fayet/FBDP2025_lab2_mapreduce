package com.bigdata.task4.merchant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MerchantDriver {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: MerchantDriver <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        Path tempOutput = new Path(args[1] + "_temp");
        
        // ========== Stage 1: 统计每个商家的发券量和使用量 ==========
        Job job1 = Job.getInstance(conf, "Merchant Analysis - Stage 1");
        job1.setJarByClass(MerchantDriver.class);
        job1.setMapperClass(MerchantStage1Mapper.class);
        job1.setReducerClass(MerchantStage1Reducer.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempOutput);
        
        if (!job1.waitForCompletion(true)) {
            System.err.println("Stage 1 failed!");
            System.exit(1);
        }
        
        // ========== Stage 2: 按活跃度分类并统计 ==========
        Job job2 = Job.getInstance(conf, "Merchant Analysis - Stage 2");
        job2.setJarByClass(MerchantDriver.class);
        job2.setMapperClass(MerchantStage2Mapper.class);
        job2.setReducerClass(MerchantStage2Reducer.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        boolean success = job2.waitForCompletion(true);
        
        // 清理临时文件
        FileSystem fs = FileSystem.get(conf);
        fs.delete(tempOutput, true);
        
        System.exit(success ? 0 : 1);
    }
}
