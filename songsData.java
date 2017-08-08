package songsList;


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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class songsData {
	
	public class records{
	public static final int artist_ID = 0;
	public static final int album_ID = 1;
	public static final int play_count = 2;
	
	}
	
	public static class songsDataMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		
		IntWritable album = new IntWritable();
		IntWritable artistID = new IntWritable();
		
		public void map (Object key, Text value,
				Mapper<Object, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String[] parts = value.toString().split("[,]");
			album.set(Integer.parseInt(parts[records.album_ID]));
			artistID.set(Integer.parseInt(parts[records.artist_ID]));
			context.write(album, artistID);
		}
				}
	
	public static class songsDataReducer extends Reducer<IntWritable, IntWritable, IntWritable,IntWritable> {
		public void reduce(
				IntWritable album,
				Iterable<IntWritable> artistIds,
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			Set<Integer>userSet = new HashSet<Integer>();
			for(IntWritable artistID : artistIds){
				
			    userSet.add(artistID.get());
			}
			IntWritable size = new IntWritable(userSet.size());
			context.write(album, size);
			
		}
	}
	
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "uniqueusers");
		
		job.setJarByClass(songsData.class);
		job.setMapperClass(songsDataMapper.class);
		job.setReducerClass(songsDataReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path OutputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		
		OutputPath.getFileSystem(conf).delete(OutputPath, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}

}



