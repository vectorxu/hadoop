package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountTest {

	public static class MyMapper extends
			Mapper<LongWritable, Text, Text, LongWritable> {
		Text k2 = new Text();
		LongWritable v2 = new LongWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] splited = line.split("\\s");
			for (String word : splited) {
				k2.set(word);
				v2.set(1);
				context.write(k2, v2);
			}
		}
	}

	public static class MyReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {
		LongWritable v3 = new LongWritable();

		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2s,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			long sum = 0;
			Iterator<LongWritable> iterator = v2s.iterator();

			while (iterator.hasNext()) {
				LongWritable v2 = (LongWritable) iterator.next();
				sum += v2.get();
			}
			v3.set(sum);
			context.write(k2, v3);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, WordCountTest.class.getSimpleName());

		job.setJarByClass(WordCountTest.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 这句话是一个默认的，可以不写，意思是使用TextInputFormat作为输入处理类，处理普通文本文件
		job.setInputFormatClass(TextInputFormat.class);

		// 设置分区
		/*
		 * job.setPartitionerClass(HashPartitioner.class);
		 * job.setNumReduceTasks(1);
		 */

		String[] arg = {"hdfs://crxy254/user/18500813893/wordcout/input"};
		// 两个路径方法一
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		//FileInputFormat.addInputPath(job, new Path(arg[1]));
		// 两个路径方法二,没有实现
		// FileInputFormat.setInputPaths(job, new Path(arg [0],arg [1]));

		FileOutputFormat.setOutputPath(job, new Path("hdfs://crxy254/user/user/18500813893/wordcount/output"));

		job.waitForCompletion(true);

	}

}
