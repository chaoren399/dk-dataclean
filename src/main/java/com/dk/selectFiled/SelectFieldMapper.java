package com.dk.selectFiled;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SelectFieldMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	String spStr ;
	String fdAr ;
	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		spStr = context.getConfiguration().get("spStr");
		fdAr=context.getConfiguration().get("fdAr");
	}
	//整数数组，内容是要保留的字段序号，没有编号的字段将去除
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		StringBuffer sb = new StringBuffer();
		
		String[] fileds = line.split(spStr);
		String[] split = fdAr.split(",");
		
		for (String string : split) {
			int index = Integer.parseInt(string)-1;
			if (sb.toString().equals("")) {
				sb.append(fileds[index]);
			}else
			sb.append(spStr).append(fileds[index]);
		}
				
		context.write(NullWritable.get(), new Text(sb.toString()));
	}

}
