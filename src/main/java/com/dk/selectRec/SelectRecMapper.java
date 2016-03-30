package com.dk.selectRec;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SelectRecMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {
	
	String spStr ;
	String compStr  ;
	String whereStr ;
	int fdNum ;
	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		spStr = context.getConfiguration().get("spStr");
		compStr = context.getConfiguration().get("compStr");
		whereStr=context.getConfiguration().get("whereStr");
		fdNum=Integer.parseInt(context.getConfiguration().get("fdNum"));
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();

		String[] fileds = line.split(spStr);
		
		//如果字段满足条件串将输出	
		switch (compStr.toLowerCase()) {
		case "eq":
			if (fileds[fdNum-1].compareTo(whereStr)==0) {
				context.write(NullWritable.get(), value);
			}
			break;
		case "ne":
			if (fileds[fdNum-1].compareTo(whereStr)!=0) {
				context.write(NullWritable.get(), value);
			}
			break;
		case "gt":
			if (fileds[fdNum-1].compareTo(whereStr)>0) {
				context.write(NullWritable.get(), value);
			}
			break;
		case "ge":
			if (fileds[fdNum-1].compareTo(whereStr)>=0) {
				context.write(NullWritable.get(), value);
			}
			break;
		case "lt":
			if (fileds[fdNum-1].compareTo(whereStr)<0) {
				context.write(NullWritable.get(), value);
			}
			break;
		case "le":
			if (fileds[fdNum-1].compareTo(whereStr)<=0) {
				context.write(NullWritable.get(), value);
			}
			break;
		case "in":
			if (fileds[fdNum-1].contains(whereStr)) {
				context.write(NullWritable.get(), value);
			}
			break;
		case "notin":
			if (!fileds[fdNum-1].contains(whereStr)) {
				context.write(NullWritable.get(), value);
			}
			break;

		default:
			break;
		}
		

		
	}
}
