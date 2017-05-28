package com.feigu;

import java.io.IOException;

import org.apache.hadoop.io.Text ;
import org.apache.hadoop.mapreduce.Mapper;

public class USDQuoteValidationMapper extends Mapper<Object, Text, Text, Text> {
	
	private Text ccy_pair = new Text();
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			
	    String line = value.toString();
	    String datas[] = line.split(",", -1);
		if (datas.length == 4) 	{
	        String key_val = datas[0];
	        //只把USD.CNY的货币对选出来,把整行内容放入context中
	        if ("USD.CNY".equals(key_val)){
	        	ccy_pair.set(key_val);
	        	context.write(ccy_pair,  value);
	        }
		}
	}
}
