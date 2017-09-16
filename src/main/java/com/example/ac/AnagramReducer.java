package com.example.ac;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Reducer that concatenates all strings in a file to
 * */
public class AnagramReducer extends Reducer<IntWritable,Text,Text,Text> {
	private Text result;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// Combine the anagrams into a single group.
		String groupedAnagrams = "";
		for (Text val : values){
			groupedAnagrams += val.toString() + " ";
		}
		result = new Text(groupedAnagrams);
		context.write(new Text(""), result);
	}
}
