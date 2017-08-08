import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class displayColdAndHot {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		public static final int MISSING = 9999;
		
		public void map(LongWritable arg0, Text value,
                Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			if(!(line.length()==0)){
				String date = line.substring(6, 14);
				float Max_temp= Float.parseFloat(line.substring(39, 45).trim());
				float Min_temp = Float.parseFloat(line.substring(47, 53).trim());
				
				if(Max_temp>35.0 && Max_temp!=MISSING)
				{
				   context.write (new Text("Sunnyday"+date), 
						   new Text(String.valueOf(Max_temp)));
				}
				
				if(Min_temp<10.0 && Min_temp!=MISSING)
				{
					context.write(new Text("Coolweather" +date),
							new Text(String.valueOf(Min_temp)));
				}
				
				
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		
	}
	
	
	
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "weatherreport");
		
		job.setJarByClass(displayColdAndHot.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
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
