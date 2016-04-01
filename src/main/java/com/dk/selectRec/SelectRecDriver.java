package com.dk.selectRec;

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


public class SelectRecDriver extends Configured implements Tool {
/**
 * 筛选记录
		spStr分隔符号，
		whereStr：条件串（大于、小于、等于），
		fdNum：字段编号，
		srcDirName：源目录名，
		dstDirName输出目录名，
		threadNum：表示启用的线程数
 */
	public int run(String[] args) throws Exception {
		if (args.length!=7){//参数检查放到调用的方法中
			System.out.println("error,Useage: hadoop jar xxx.jar [mainclass] spStr compStr whereStr fdNum srcDirName dstDirName threadNum");
			return -1;
		}
		
		String spStr=args[0];
		String compStr=args[1];
		String whereStr=args[2];
		String fdNum=args[3];
		String srcDirName=args[4];
		String dstDirName=args[5];
		String threadNum=args[6];
		
		Configuration conf = new Configuration();
		
		conf.set("spStr", spStr);
		conf.set("compStr", compStr);
		conf.set("whereStr", whereStr);
		conf.set("fdNum", fdNum);
		conf.set("srcDirName", srcDirName);
		conf.set("dstDirName", dstDirName);
		conf.set("threadNum", threadNum);
				
		Job job = Job.getInstance(conf, SelectRecDriver.class.getSimpleName());
		job.setJarByClass(SelectRecDriver.class);
		job.setNumReduceTasks(Integer.parseInt(threadNum));
		
		FileInputFormat.setInputPaths(job, srcDirName);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(SelectRecMapper.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(dstDirName));
		
		return job.waitForCompletion(true)?0:1;
		
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SelectRecDriver(), args);
	}

}
