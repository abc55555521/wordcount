/**
 * 
 */
package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author lyl
 *
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>  {
	 
	private Text outputKey=new Text();
	private IntWritable outputValue=new IntWritable(1);
	
	@Override
	protected void map(LongWritable key,Text value,Context context ) throws IOException, InterruptedException {
		String line=value.toString();
		String[] words=line.split(" ");
		
		for(String word:words) {
			outputKey.set(word);
			context.write(outputKey, outputValue);
		}
	}
}
