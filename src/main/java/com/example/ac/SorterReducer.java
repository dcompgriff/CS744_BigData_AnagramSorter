package com.example.ac;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

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
