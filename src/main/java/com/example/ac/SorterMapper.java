package com.example.ac;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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
