package com.feigu;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USDQuoteMapper
             extends Mapper<Text, Text, Text, DoubleWritable> {
	
	private Text ccy_pair = new Text();
	
	@Override
	protected void map(Text key, Text value, Context context)//key是Text类型
		throws IOException, InterruptedException {
		
	    String line = value.toString();
	    String datas[] = line.split(",", -1);
		if (datas.length == 4) 	{
	        String key_val = datas[0];
			ccy_pair.set(key_val);
			double quote = parseDouble(datas[2]);
			if (quote != -1){
				context.write(ccy_pair,  new DoubleWritable(quote));//把第一列作为key
			}
		}
	}

	/***
	 * 判断是否是浮点数字
	 * 
	 * @param num
	 * @return
	 */
	private double parseDouble(String num){
		try {
			return Double.parseDouble(num);
		}
		catch(Exception exp){
			return -1;
		}
	}

}
