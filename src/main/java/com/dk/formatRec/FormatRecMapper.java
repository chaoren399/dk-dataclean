package com.dk.formatRec;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FormatRecMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] fileds = line.split(context.getConfiguration().get("spStr"));
		//过滤掉字段数量不符合的
		if(fileds.length==Integer.parseInt(context.getConfiguration().get("fdSum"))){
			context.write(NullWritable.get(), value);
		}
		
	}
}
