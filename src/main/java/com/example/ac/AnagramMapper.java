package com.example.ac;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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
