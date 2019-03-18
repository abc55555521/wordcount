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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lyl
 *
 */
public class WordCountDriver {
	
	static Logger logger= LoggerFactory.getLogger(WordCountDriver.class);
	
	/*
	 * 常用命令:
	 * 上传文件:hadoop fs -put wordcount.txt /wordcount/input
	 * 执行命令:hadoop jar wordcount.jar com.mapreduce.wordcount.WordCountDriver /wordcount/input /wordcount/output
	 * 结果查看:hadoop fs -cat /wordcount/output/part-r-00000
	 * 删除结果:hadoop dfs -rm  -R -f /wordcount/output/
	 * jar包可以在任何一台hadoop上执行
	 * */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	 	/*	args = new String[] {
					"hdfs://hadoopslave1:9000/wordcount/input",
					"hdfs://hadoopslave1:9000/wordcount/output"
				}; */
		if (args != null || args.length < 2) {
			System.out.println("main函数参数传递不正确！！！");
			logger.error("main函数参数不正确！！！");
		}
 
		
		//提示用户不正确 Permission denied: user=Administrator, access=EXECUTE, inode="/tmp":hadoop:supergroup:drwx
		/*	hdfs-site.xml设定不验证	 
		 *  <property>
	        <name>dfs.permissions</name>
	        <value>false</value>
	    </property>	*/
		// Run Configurations  中：Arguments -->VM  arguments: -DHADOOP_USER_NAME=hadoop
		//也可以直接hadoop fs -chmod -R 777 /
		// System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		// hadoop默认配置参数
		Configuration conf = new Configuration();
		//conf.set("fs.defaultFS", "hdfs://hadoopsalve1:9000");
		
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
