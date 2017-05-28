package com.feigu;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class USDMaxQuote extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(MaxQuote.class);
		job.setJobName("USDMaxQuote");
		
		//定义一个Mapper链,把筛选货币对的Mapper和做映射的Mapper依次放到链中
		ChainMapper.addMapper(job, USDQuoteValidationMapper.class, Object.class, Text.class, 
				Text.class, Text.class, job.getConfiguration());
		ChainMapper.addMapper(job, USDQuoteMapper.class, Text.class, Text.class, 
				Text.class, DoubleWritable.class, job.getConfiguration());
		
		//定义Job的Mapper类是ChainMapper
		job.setMapperClass(ChainMapper.class);
		job.setReducerClass(MaxQuoteReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		
		return success?0:1;
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
		      System.err.println("Usage: USDMaxQuote <input hdfs path> <output hdfs path>");
		      System.exit(-1);
		}
	    
	    int val = ToolRunner.run(new USDMaxQuote(), args);
	    System.exit(val);
	}

}
