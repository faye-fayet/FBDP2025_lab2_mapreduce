package com.bigdata.task4.weekday;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeekdayReducer extends Reducer<Text, Text, Text, Text> {
    
    private Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        int usedCount = 0;
        int unusedCount = 0;
        
        for (Text value : values) {
            String status = value.toString();
            if ("used".equals(status)) {
                usedCount++;
            } else if ("unused".equals(status)) {
                unusedCount++;
            }
        }
        
        int totalCount = usedCount + unusedCount;
        double usageRate = totalCount > 0 ? (double) usedCount / totalCount * 100 : 0;
        
        // 输出格式：日期类型 TAB 领取数,使用数,使用率%
        result.set(String.format("领取:%d, 使用:%d, 使用率:%.2f%%", 
                   totalCount, usedCount, usageRate));
        
        context.write(key, result);
    }
}
