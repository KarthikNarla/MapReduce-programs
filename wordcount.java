import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class WordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	    @Override
	    public void map(LongWritable key, Text value,
	                    Context context) throws IOException, InterruptedException {
	      String line = value.toString();
	      StringTokenizer tokenizer = new StringTokenizer(line);
	      while (tokenizer.hasMoreTokens()) {
	        value.set(tokenizer.nextToken());
	        context.write(value, new IntWritable(1));
	      }
	    }
	  }
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	    @Override
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable x : values) {
	        sum += x.get();
	      }

	      context.write(key, new IntWritable(sum));
	    }
	  }
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf  = new Configuration();
		Job job = Job.getInstance(conf, "wordcount");
		
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		
	}
}
