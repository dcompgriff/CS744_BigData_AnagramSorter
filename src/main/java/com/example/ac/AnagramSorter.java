import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.example.ac.AnagramMapper;
import com.example.ac.AnagramReducer;

public class AnagramSorter {

	/**
	 * Mapper that maps words to (length, word)//(length:sorted(word), word) tuples.
	 * */
	public class AnagramMapper extends Mapper<Object, Text, Text, Text>{
		private IntWritable numberOfCharacters;
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Convert each word to its character sorted form
			// and output (char sorted word, word)
			String sortedWord = "";
			char[] sortedChars;
			sortedChars = value.toString().toCharArray();
			Arrays.sort(sortedChars);
			sortedWord = new String(sortedChars);
			sortedWord = sortedWord.trim();
			context.write(new Text(sortedWord), value);
		}
	}
	
	/**
	 * Reducer that concatenates all strings in a file to
	 * */
	public class AnagramReducer extends Reducer<Text,Text,Text,Text> {
		private Text result;

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// Combine the anagrams into a single group.
			String groupedAnagrams = "";
			for (Text val : values){
				groupedAnagrams += val.toString() + " ";
			}
			result = new Text(groupedAnagrams);
			context.write(new Text(""), result);
		}
		
		
	}
	
	/**
	 * Mapper that maps words to (length, word)//(length:sorted(word), word) tuples.
	 * */
	public class SorterMapper extends Mapper<Object, Text, IntWritable, Text>{
		private IntWritable numberOfCharacters;

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Break each line into its list of anagrams, and create an Integer, Array
			//LinkedList<String> anagramList = new LinkedList<String>();
			String[] stringList = value.toString().split(" ");
			for(int i=0; i<stringList.length; i++){
				stringList[i] = stringList[i].trim();
			}
			
			// Write the (integer, array) tuple to the context.
			context.write(new IntWritable(-stringList.length), value);
		}
	}
	
	/**
	 * Reducer that concatenates all strings in a file to
	 * */
	public class SorterReducer extends Reducer<IntWritable, Text,NullWritable,Text> {
		private Text result;
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// Combine the anagrams into a single group.
			for (Text val : values){
			    context.write(NullWritable.get(), val);
			}
		}
		
	}
	
	/**
	 * Main function that sets up the anagram sorting run (with 1 map and reduce stage).
	 * */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Run first stage for creating anagrams.
		int returnCode = 0;
		Job job1 = Job.getInstance(conf, "AnagramGeneration");
		job1.setJarByClass(AnagramSorter.class);
		job1.setMapperClass(AnagramSorter.AnagramMapper.class);
		job1.setReducerClass(AnagramSorter.AnagramReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		UUID newTempFile = UUID.randomUUID();
		String tempUUIDFilePath = newTempFile.toString();
		FileOutputFormat.setOutputPath(job1, new Path(tempUUIDFilePath));
		returnCode = job1.waitForCompletion(true) ? 0 : 1;
		if(returnCode == 1){
			System.exit(returnCode);
		}

		// Run second stage for sorting anagram groups.
		conf = new Configuration();
		Job job2 = Job.getInstance(conf, "AnagramSorting");
		job2.setJarByClass(AnagramSorter.class);
		job2.setMapperClass(AnagramSorter.SorterMapper.class);
		job2.setReducerClass(AnagramSorter.SorterReducer.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(tempUUIDFilePath));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		returnCode = job2.waitForCompletion(true) ? 0 : 1;
		System.exit(returnCode);
	}	
}
