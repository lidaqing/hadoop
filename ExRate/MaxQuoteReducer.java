package com.feigu;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxQuoteReducer
             extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
	     double maxVal = 0.0;
	     for (DoubleWritable value : values){
	    	 maxVal = Math.max(maxVal, value.get());
	     }
		 context.write(key, new DoubleWritable(maxVal));
	}

}
