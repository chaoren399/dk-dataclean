package com.dk.formatRec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FormatRecDriver extends Configured implements Tool{
	/**
	 * 规范记录
		spStr分隔符号，
		fdSum：字段数量（不符合该数量的记录将被清除），
		srcDirName：源目录名，
		dstDirName输出目录名，
		threadNum：表示启用的线程数
	 */
	public static String spStr;
	public static String fdSum;
	public static String srcDirName;
	public static String dstDirName;
	public static String threadNum;
	

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FormatRecDriver(), args);
	}

	public int run(String[] args) throws Exception {
		
		if (args.length!=5){
			System.out.println(" ERROR,please check your input!\n Useage: hadoop jar xxx.jar [mainclass] spStr fdSum srcDirName dstDirName threadNum");
			return -1;
		}
		
		spStr=args[0];
		fdSum=args[1];
		srcDirName=args[2];
		dstDirName=args[3];
		threadNum=args[4];
				
//		System.out.println(spStr+ fdSum+  srcDirName+  dstDirName+  threadNum);
		
		Configuration conf = new Configuration();
		
		conf.set("spStr", spStr);
		conf.set("fdSum", fdSum);
		conf.set("srcDirName", srcDirName);
		conf.set("dstDirName", dstDirName);
		conf.set("threadNum", threadNum);
				
		Job job = Job.getInstance(conf, FormatRecDriver.class.getSimpleName());
		job.setJarByClass(FormatRecDriver.class);
		job.setNumReduceTasks(Integer.parseInt(threadNum));
		
		FileInputFormat.setInputPaths(job, srcDirName);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(FormatRecMapper.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		/*
		job.setReducerClass(FormatRecReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		*/
		FileOutputFormat.setOutputPath(job, new Path(dstDirName));
		
		return job.waitForCompletion(true)?0:1;

	}

}
