package com.dk.formatField;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FormatFieldMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] fileds = line.split(context.getConfiguration().get("spStr"));
		String regExStr=context.getConfiguration().get("regExStr");
		int fdNum=Integer.parseInt(context.getConfiguration().get("fdNum"));
		//符合该表达式字段的记录将被剔除
		if (fdNum==0){
			Pattern pattern = Pattern.compile(regExStr);
			Matcher matcher = pattern.matcher(line);
			
			if (!matcher.matches()) {//如果没有匹配到，写出
				context.write(NullWritable.get(), value);
			}
			
		}else {
			Pattern pattern = Pattern.compile(regExStr);
			Matcher matcher = pattern.matcher(fileds[fdNum-1]);
			
			if (!matcher.matches()) {
				context.write(NullWritable.get(), value);
			}
		}
		
	}

}
