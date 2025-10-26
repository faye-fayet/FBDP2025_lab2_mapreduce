package com.bigdata.task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CouponTimeDriver {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: CouponTimeDriver <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        
        // 临时输出路径
        Path tempOutput = new Path(args[1] + "_temp");
        
        // ========== Job 1: 统计每个优惠券的使用次数和总间隔 ==========
        Job job1 = Job.getInstance(conf, "Coupon Time Statistics - Stage 1");
        job1.setJarByClass(CouponTimeDriver.class);
        job1.setMapperClass(CouponTimeMapper.class);
        job1.setReducerClass(CouponTimeReducer.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tempOutput);
        
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 failed!");
            System.exit(1);
        }
        
        // ========== Job 2: 过滤和计算平均间隔 ==========
        Job job2 = Job.getInstance(conf, "Coupon Time Statistics - Stage 2");
        job2.setJarByClass(CouponTimeDriver.class);
        job2.setMapperClass(FilterAndAverageMapper.class);
        job2.setReducerClass(FilterAndAverageReducer.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        // 设置总使用次数阈值（需要在第二个Job中使用）
        job2.getConfiguration().set("temp.output.path", tempOutput.toString());
        
        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        boolean success = job2.waitForCompletion(true);
        
        // 清理临时文件
        FileSystem fs = FileSystem.get(conf);
        fs.delete(tempOutput, true);
        
        System.exit(success ? 0 : 1);
    }
}
