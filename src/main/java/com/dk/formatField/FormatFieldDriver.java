package com.dk.formatField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FormatFieldDriver extends Configured implements Tool {
	/**
	 * 规范字段
		spStr分隔符号，
		regExStr正则表达式（符合该表达式字段的记录将被剔除），
		fdNum：字段序号（检查哪个字段是否符合正则，0为全部检查），
		srcDirName：源目录名，
		dstDirName输出目录名，
		threadNum：表示启用的线程数
	 */

	public int run(String[] args) throws Exception {
		if (args.length!=6){//参数检查放到调用的方法中
			System.out.println(" ERROR,please check your input!\n Useage: hadoop jar xxx.jar [mainclass] spStr regExStr fdNum srcDirName dstDirName threadNum");
			return -1;
		}
		
		String spStr=args[0];
		String regExStr=args[1];
		String fdNum=args[2];
		String srcDirName=args[3];
		String dstDirName=args[4];
		String threadNum=args[5];
		
		Configuration conf = new Configuration();
		
		conf.set("spStr", spStr);
		conf.set("regExStr", regExStr);
		conf.set("fdNum", fdNum);
		conf.set("srcDirName", srcDirName);
		conf.set("dstDirName", dstDirName);
		conf.set("threadNum", threadNum);
				
		Job job = Job.getInstance(conf, FormatFieldDriver.class.getSimpleName());
		job.setJarByClass(FormatFieldDriver.class);
		job.setNumReduceTasks(Integer.parseInt(threadNum));
		
		FileInputFormat.setInputPaths(job, srcDirName);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(FormatFieldMapper.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(dstDirName));
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new FormatFieldDriver(), args);
	}

}
