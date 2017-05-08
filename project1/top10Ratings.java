package bigDataAssignment;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import bigDataAssignment.HomeworkPart1.Mapclass1;
import bigDataAssignment.HomeworkPart1.Redclass1;

public class HomeworkPart2 {
	public static class MapClass2_1 extends MapReduceBase implements Mapper<LongWritable,Text,Text,FloatWritable>
	{
		private Text words = new Text();
		public void map(LongWritable key,Text value,OutputCollector<Text,FloatWritable> outs,Reporter rep)throws IOException
		{
			String l = value.toString();
			String[] st = l.split("::");
			words.set(st[2]);
			outs.collect(words, new FloatWritable(Float.parseFloat(st[3])));
		}
	}

	
	public static class RedClass2_2 extends MapReduceBase implements Reducer<Text,FloatWritable,Text,FloatWritable>
	{
		public void reduce(Text keys,Iterator<FloatWritable> vals ,OutputCollector<Text ,FloatWritable> outs,Reporter rep) throws IOException
		{
			FloatWritable avg = new FloatWritable();
			int summer = 0;
			float counter = 0;
			while(vals.hasNext())
			{
				summer += vals.next().get();
				counter++;
			}
			float average = summer/counter;
			avg.set(average);
			outs.collect(keys, avg);
		}		
	}	
	

	public static class MapClass2_3 extends MapReduceBase implements Mapper<LongWritable,Text,FloatWritable,Text>
	{		
		TreeMap<String,Float> treemap = new TreeMap<String,Float>();
		public void map(LongWritable key,Text val,OutputCollector<FloatWritable,Text> outs,Reporter rep) throws IOException
		{
			String[] st = val.toString().trim().split("\t");
			String k = st[0];
			float v = Float.parseFloat(st[1]);
				outs.collect(new FloatWritable(1),new Text(v+"::"+st[0]));		
			}
		}
	

	public static class RedClass2_4 extends MapReduceBase implements Reducer<FloatWritable,Text,Text, FloatWritable>
	{
		TreeMap<Float,ArrayList<String>> treeMap = new TreeMap<Float,ArrayList<String>>(Collections.reverseOrder());
		int count = 10;
		public void reduce(FloatWritable key,Iterator<Text> val,OutputCollector<Text, FloatWritable> outs,Reporter rep) throws IOException
		{
			String str = new String();		
			while(val.hasNext())
			{
				String st[]  = val.next().toString().split("::");
				Float k = Float.parseFloat(st[0]);
				if(treeMap.containsKey(k))
					treeMap.get(k).add(st[1]);
				else
				{
					ArrayList<String> arrList = new ArrayList();
					arrList.add(st[1]);
					treeMap.put(k, arrList);
				}
		    }
			int count = 0;
			while(count < 10)
			{
				Map.Entry<Float, ArrayList<String>> entry = treeMap.pollFirstEntry();
				for(String businessString : entry.getValue())
				{
					if(count == 10)
						break;
					outs.collect(new Text(businessString), new FloatWritable(entry.getKey()));
					count++;
				}
				
			}
		}
	}
	
	public static void main (String[] args) throws IOException
	{
		JobConf config = new JobConf(HomeworkPart2.class);
		config.setJobName("HomeWorkPart2");
		config.setMapOutputKeyClass(Text.class);
		config.setMapOutputValueClass(FloatWritable.class);
		config.setOutputValueClass(FloatWritable.class);
		config.setOutputKeyClass(Text.class);
		config.setMapperClass(MapClass2_1.class);
		config.setReducerClass(RedClass2_2.class);
		FileInputFormat.setInputPaths(config, new Path(args[0]));
		FileOutputFormat.setOutputPath(config, new Path( args[1]));
		// run 2
		JobConf config_1 = new JobConf(HomeworkPart2.class);
		config_1.setJobName("HomeWorkPart2_1");
		config_1.setMapOutputKeyClass(FloatWritable.class);
		config_1.setMapOutputValueClass(Text.class);
		config_1.setOutputValueClass(FloatWritable.class);
		config_1.setOutputKeyClass(Text.class);
		config_1.setMapperClass(MapClass2_3.class);
		config_1.setReducerClass(RedClass2_4.class);
		FileInputFormat.setInputPaths(config_1, new Path(args[1]+"/part-00000"));
		FileOutputFormat.setOutputPath(config_1, new Path(args[2]));		

		RunningJob rj = JobClient.runJob(config);
		rj.waitForCompletion();
		JobClient.runJob(config_1);
	}
}
