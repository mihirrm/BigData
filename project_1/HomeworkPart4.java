package bigDataAssignment;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HomeworkPart4 
{	
	public static class DistributedMap extends 	Mapper<LongWritable, Text,Text,Text>
	{
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text(); // type of output key
		private Text rating = new Text(); // type of output key
		private HashSet<String> hmap = new HashSet<String>();
		protected void setup(Context context) throws IOException,InterruptedException 
		{
			try 
			{
					Path[] Files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
						if (Files != null && Files.length > 0) 
						{
							for (Path i : Files) 
							{
								readFile(i,context);
							}
						}
			} 
			catch (Exception ex) 
			{
				System.err.println("Exception in mapper setup: "+ ex.getMessage());
			}
		}

		public void map(LongWritable key, Text value, Context outs)throws IOException, InterruptedException 
		{
			String[] line = value.toString().split(" ");
			for (String item : line)
			{
				String[] reviews = item.split("::");
				if (hmap.contains(reviews[2])) 
				{
					word.set(reviews[1]);
					rating.set(reviews[3]);
					outs.write(word, rating);
				}
			}
		}

		private void readFile(Path filePath,Context con) 
		{
			try 
			{
				FileReader fr = new FileReader(filePath.toString());
				BufferedReader bufferedReader = new BufferedReader(fr);
				String lines ;
				while ((lines = bufferedReader.readLine()) != null) 
				{
					String[] arr = lines.split("::"); 	
					//String addr_split = arr[1].substring(0, arr[1].lastIndexOf(","));
					
					if(arr[1].contains("Stanford"))
						{
							hmap.add(arr[0]);							
						}
					}
			}
			catch (Exception e)
			{
				System.err.println("Exception while reading stop words file: "+ e.getMessage());
			}
		}
	}
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args)throws IOException
	{
		Configuration c = new Configuration();
		Job job = new Job(c);
		job.setJarByClass(HomeworkPart4.class);
		job.setJobName("In Memory Join");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
		job.setMapperClass(DistributedMap.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));		
	
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
		try 
		{
			job.waitForCompletion(true);
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
		} catch (InterruptedException e) 
		{
			e.printStackTrace();
		}
	}
}