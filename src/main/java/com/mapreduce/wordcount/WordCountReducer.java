/**
 * 
 */
package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author lyl
 *
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	private IntWritable outputValue=new IntWritable();
			
	@Override
	protected void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int count=0;
		for(IntWritable value:values) {
			count+=value.get();
		}
		outputValue.set(count);
		context.write(key, outputValue);
	}
}
