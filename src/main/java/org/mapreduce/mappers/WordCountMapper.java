package org.mapreduce.mappers;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		String line = lineText.toString();
		Text currentWord = new Text();
		for (String word : line.split("\\W")) {
			if (word.isEmpty()) {
				continue;
			}
			currentWord = new Text(word);
			context.write(currentWord, one);
		}

	}
}
