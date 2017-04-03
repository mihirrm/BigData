package bigDataAssignment;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;

import bigDataAssignment.HomeworkPart2.MapClass2_1;
import bigDataAssignment.HomeworkPart2.MapClass2_3;
import bigDataAssignment.HomeworkPart2.RedClass2_2;
import bigDataAssignment.HomeworkPart2.RedClass2_4;

public class HomeworkPart3 {
	
	// first job is to load review.csv and get average rating which outputs distinct id with average rating
	
	public static class MapClassReviewLoad extends MapReduceBase implements Mapper<LongWritable,Text,Text,FloatWritable>
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

	
	public static class RedClassReviewMerge extends MapReduceBase implements Reducer<Text,FloatWritable,Text,FloatWritable>
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
	
		// second job to calculate top 10 average ratings
	public static class MapClass2ReviewAverage extends MapReduceBase implements Mapper<LongWritable,Text,FloatWritable,Text>
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
	

	public static class RedClass2ReviewAverage extends MapReduceBase implements Reducer<FloatWritable,Text,Text, FloatWritable>
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
	
	
	
	
	
	// third job to merge business.csv with the top 10 average rating
	
	public static class MapclassReview extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,OutputCollector<Text,Text> outs,Reporter rep)throws IOException
		{
			Text key_ins = new Text();
			Text val_ins = new Text();
			String l = value.toString();
			String[] arr = l.split("\t");
			key_ins.set(arr[0]);
			String ins = arr[1]+"::review ";
			val_ins.set(ins);
			outs.collect(key_ins,val_ins);
		}
	}

	
	public static class MapclassBussiness extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,OutputCollector<Text,Text> outs,Reporter rep)throws IOException
		{
			Text key_ins = new Text();
			Text val_ins = new Text();
			String l = value.toString();
			String[] arr = l.split("::");
			key_ins.set(arr[0]);
			val_ins.set(arr[1]+"   "+arr[2]+"::business");
			outs.collect(key_ins,val_ins);				
		}
	}
	
	
	
	
	
	public static class RedclassMerge extends MapReduceBase implements Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text keys, Iterator<Text> vals, OutputCollector<Text,Text> outs,Reporter rep) throws IOException
		{
			String st = "";
			ArrayList<String> arr_review  = new ArrayList<String>();
			ArrayList<String> arr_business  = new ArrayList<String>();
			
			while(vals.hasNext())
				{
				 	st = vals.next().toString();
				 	if (st.endsWith("::business"))
				 	{
				 		arr_business.add(st.split("::business")[0]);
				 	}
				 	else
				 	{
				 		arr_review.add(st.split("::review")[0]);
				 	}
				}
			
			outer :for (int i=0;i<arr_review.size();i++)
			{
				for(int j=0;j<arr_business.size();j++)
				{
					String str =arr_business.get(j)+"   "+ arr_review.get(i);
					outs.collect(new Text(keys), new Text(str));
					break outer;
				}
			}
		}
		
	}
	
	
		public static void main (String[] args) throws IOException
		{
			// job_1
			JobConf config = new JobConf(HomeworkPart3.class);
			config.setJobName("Reduce Join");
			config.setOutputKeyClass(Text.class);
			config.setOutputValueClass(FloatWritable.class);
			config.setReducerClass(RedClassReviewMerge.class);
			config.setMapperClass(MapClassReviewLoad.class);
			FileInputFormat.setInputPaths(config, new Path(args[0]));
			FileOutputFormat.setOutputPath(config, new Path( args[1]));
			RunningJob rj = JobClient.runJob(config);
			rj.waitForCompletion();
			
			
			//job_2
			JobConf config_1 = new JobConf(HomeworkPart3.class);
			config_1.setJobName("HomeWorkPart2_1");
			config_1.setMapOutputKeyClass(FloatWritable.class);
			config_1.setMapOutputValueClass(Text.class);
			config_1.setOutputValueClass(FloatWritable.class);
			config_1.setOutputKeyClass(Text.class);
			config_1.setMapperClass(MapClass2ReviewAverage.class);
			config_1.setReducerClass(RedClass2ReviewAverage.class);
			FileInputFormat.setInputPaths(config_1, new Path(args[1]+"/part-00000"));
			FileOutputFormat.setOutputPath(config_1, new Path(args[2]));		
			RunningJob rj_1 = JobClient.runJob(config_1);
			rj_1.waitForCompletion();
			
			
			
			//job_3
			JobConf config_2 = new JobConf(HomeworkPart3.class);
			config_2.setJobName("Reduce Join");
			config_2.setOutputKeyClass(Text.class);
			config_2.setOutputValueClass(Text.class);
			config_2.setReducerClass(RedclassMerge.class);
			MultipleInputs.addInputPath(config_2, new Path(args[2]+"/part-00000"), TextInputFormat.class,MapclassReview.class);
			MultipleInputs.addInputPath(config_2, new Path(args[3]), TextInputFormat.class,MapclassBussiness.class);			
			FileOutputFormat.setOutputPath(config_2, new Path(args[4]));
			JobClient.runJob(config_2);
			
		}
			
}


