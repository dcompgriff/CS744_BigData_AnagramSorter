package com.example.ac;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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
