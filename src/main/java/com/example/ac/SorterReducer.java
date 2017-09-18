package com.example.ac;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Reducer that concatenates all strings in a file to
 * */
public class SorterReducer extends Reducer<IntWritable,ArrayWritable,Text,Text> {
	private Text result;

	@Override
	protected void reduce(IntWritable key, Iterable<ArrayWritable> values, Reducer<IntWritable, ArrayWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// Combine the anagrams into a single group.
		String groupedAnagrams = "";
		for (ArrayWritable val : values){
			String[] anagramList = val.toStrings();
			groupedAnagrams = "";
			for(int i=0; i<anagramList.length; i++){
				groupedAnagrams += val.toString() + " ";
			}
			context.write(new Text(""), new Text(groupedAnagrams));
		}
	}
	
}
