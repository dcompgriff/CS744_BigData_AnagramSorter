package com.example.ac;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramSorter {

	/**
	 * Mapper that maps words to (length, word)//(length:sorted(word), word) tuples.
	 * */
	public static class AnagramMapper extends Mapper<Object, Text, Text, Text>{
		private IntWritable numberOfCharacters;
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Convert each word to its character sorted form
			// and output (char sorted word, word)
			String sortedWord = "";
			char[] sortedChars;
			sortedChars = value.toString().toCharArray();
			Arrays.sort(sortedChars);
			sortedWord = new String(sortedChars);
			context.write(new Text(sortedWord), value);
		}
	}

	/**
	 * Reducer that concatenates all strings in a file to
	 * */
	public static class AnagramReducer extends Reducer<IntWritable,Text,Text,Text> {
		private Text result;
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Combine the anagrams into a single group.
			String groupedAnagrams = "";
			for (Text val : values){
				groupedAnagrams += val.toString() + " ";
			}
			result = new Text(groupedAnagrams);
			context.write(key, result);
		}
	}
	
	/**
	 * Main function that sets up the anagram sorting run (with 1 map and reduce stage).
	 * */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		Configuration conf1 = new Configuration();
//		Configuration conf2 = new Configuration();
//		Job job = Job.getInstance(conf, "word count");
//		job.setJarByClass(WordCount.class);
//		job.setMapperClass(AnagramMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
//		job.setReducerClass(IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
//		// Set up the first configuration object.
//		Job job = Job.getInstance(conf, "blank");
//		job.setJarByClass(AnagramSorter.class);
//		job.setMapperClass(AnagramMapper.class);
//		job.setCombinerClass(AnagramReducer.class);
//		job.setReducerClass(AnagramReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		// Create anagram Map/Reduce job.
//		ControlledJob anagramFindingJob = new ControlledJob(job.getConfiguration());
//				
//		// Set up the first configuration object.
//		conf = new Configuration();
//		job = Job.getInstance(conf, "blank");
//		job.setJarByClass(AnagramSorter.class);
//		job.setMapperClass(AnagramMapper.class);
//		job.setCombinerClass(AnagramReducer.class);
//		job.setReducerClass(AnagramReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		// Create anagram Map/Reduce job.
//		ControlledJob anagramGroupSortingJob = new ControlledJob(job.getConfiguration());
//			
//		// Create controlled Map/Reduce job dependence.
//		anagramGroupSortingJob.addDependingJob(anagramFindingJob);
		
//		// Create the job control object.
//		JobControl jControl = new JobControl("anagram job control");
//		jControl.addJob(anagramFindingJob);
////		jControl.addJob(anagramGroupSortingJob);
//		// Create a separate thread to run the jcontrol object.
//		Thread runJControl = new Thread(jControl);
//		runJControl.start();
//		int returnCode = 0;
//		while(!jControl.allFinished()){
//			returnCode = jControl.getFailedJobList().size() == 0 ? 0 : 1;
//			Thread.sleep(100);
//		}
//		System.exit(returnCode);
		
		// Run first stage for creating anagrams.
		int returnCode = 0;
		Job job1 = Job.getInstance(conf, "AnagramGeneration");
		job1.setJarByClass(AnagramSorter.class);
		job1.setMapperClass(AnagramMapper.class);
		job1.setCombinerClass(AnagramReducer.class);
		job1.setReducerClass(AnagramReducer.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("temp"));
		returnCode = job1.waitForCompletion(true) ? 0 : 1;
		//if(returnCode == 1){
			System.exit(returnCode);
		//}

		// Run second stage for sorting anagram groups.
//		Job job2 = Job.getInstance(conf, "AnagramGeneration");
//		job2.setJarByClass(AnagramSorter.class);
//		job2.setMapperClass(AnagramMapper.class);
//		job2.setCombinerClass(AnagramReducer.class);
//		job2.setReducerClass(AnagramReducer.class);
//		job2.setOutputKeyClass(Text.class);
//		job2.setOutputValueClass(Text.class);
//
//		FileInputFormat.addInputPath(job2, new Path("temp"));
//		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
//
//		returnCode = job2.waitForCompletion(true) ? 0 : 1;
//		System.exit(returnCode);
	}	
}
