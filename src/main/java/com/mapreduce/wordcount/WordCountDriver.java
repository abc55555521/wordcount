/**
 * 
 */
package com.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author lyl
 *
 */
public class WordCountDriver {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] {
				"hdfs://hadoopmaster:9000/wordcount/input",
				"hdfs://hadoopmaster:9000/wordcount/output"
			};
				
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		// hadoop默认配置参数
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoopmaster:9000");
		
		// 获取job实例对象
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountDriver.class);

		// 数据的输入路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 数据的输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 指定使用的map和reduce方法所在类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		// 指定map输出的k-v的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 指定reduce输出的k-v对的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 提交,result是true说明执行成功了
		boolean result = job.waitForCompletion(true);  //print the progress to the user
		// 成功返回0失败返回1
		System.exit(result ? 0 : 1);

	}
}
