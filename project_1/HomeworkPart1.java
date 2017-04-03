package bigDataAssignment;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class HomeworkPart1 {

	public static class Mapclass1 extends MapReduceBase implements Mapper<LongWritable,Text, Text, Text>
	{
		private Text words = new Text();
		public void map(LongWritable key,Text value,OutputCollector<Text, Text> outs,Reporter rep)throws IOException
		{
			try
			{
				TreeSet<String> hash_set = new TreeSet<String>();
				Text word = new Text("Palo Alto");
				String l = value.toString();
				System.out.println(l);
				String[] st = l.split("::");
				if(st[1].contains("Palo Alto"))
				{
					String sub = st[2].substring(st[2].indexOf('(')+1, st[2].lastIndexOf(')'));
					if (!sub.isEmpty())
					{
						String[] arr = sub.split(",");
						for(String string : arr)
							if(string != null)
								hash_set.add(string.trim());
					}
					
				}	
				words.set(word);
				for(String s: hash_set)
					outs.collect(words, new Text(s));
			}
			catch(Exception e)
			{
				System.out.println(e);
			}
		}
	}


	public static class Redclass1 extends MapReduceBase implements Reducer<Text,Text,Text, NullWritable>
	{
		public void reduce(Text keys, Iterator<Text> vals, OutputCollector<Text, NullWritable> outs,Reporter rep) throws IOException
		{
			
			TreeSet<String> final_outs = new TreeSet<String>();
			while(vals.hasNext())
				final_outs.add(vals.next().toString());
				
			for(String s: final_outs)
				outs.collect(new Text(s), NullWritable.get());
		}
	}


	public static void main (String[] args) throws IOException
	{
		JobConf config = new JobConf(HomeworkPart1.class);
		config.setJobName("Distinct Business in Palo Alto");
		config.setMapOutputKeyClass(Text.class);
		config.setMapOutputValueClass(Text.class);		
		config.setOutputValueClass(NullWritable.class);
		config.setOutputKeyClass(Text.class);
		config.setMapperClass(Mapclass1.class);
		config.setReducerClass(Redclass1.class);		
		FileInputFormat.setInputPaths(config, new Path(args[0]));
		FileOutputFormat.setOutputPath(config, new Path(args[1]));
		JobClient.runJob(config);
	}
	}