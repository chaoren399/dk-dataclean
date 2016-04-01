package com.dk.selectFiled;

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


public class SelectFieldDriver extends Configured implements Tool {


	public int run(String[] args) throws Exception {
		if (args.length!=5){//参数检查放到调用的方法中
			System.out.println(" ERROR,please check your input!\n Useage: hadoop jar xxx.jar [mainclass] spStr fdAr srcDirName dstDirName threadNum");
			return -1;
		}
		
		String spStr=args[0];
		String fdAr=args[1];
		String srcDirName=args[2];
		String dstDirName=args[3];
		String threadNum=args[4];
		
		Configuration conf = new Configuration();
		
		conf.set("spStr", spStr);
		conf.set("fdAr", fdAr);
		conf.set("srcDirName", srcDirName);
		conf.set("dstDirName", dstDirName);
		conf.set("threadNum", threadNum);
				
		Job job = Job.getInstance(conf, SelectFieldDriver.class.getSimpleName());
		job.setJarByClass(SelectFieldDriver.class);
		job.setNumReduceTasks(Integer.parseInt(threadNum));
		
		FileInputFormat.setInputPaths(job, srcDirName);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(SelectFieldMapper.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(dstDirName));
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SelectFieldDriver(), args);
	}

}
