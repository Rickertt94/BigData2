package main.java.nl.hu.hadoop.lineCorrection;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class LineCorrection {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(LineCorrection.class);

		Path jobFileInputPath = new Path(args[0]);
		Path jobFileOutputPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, jobFileInputPath);
		FileOutputFormat.setOutputPath(job, jobFileOutputPath);

		job.setMapperClass(FirstFilterMapper.class);
		job.setReducerClass(FirstFilterReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		
		
		Job job2 = new Job();
		job2.setJarByClass(LineCorrection.class);

		Path job2FileInputPath = jobFileOutputPath;
		Path job2FileOutputPath = new Path("/usr/local/hadoop/output2");
		FileInputFormat.addInputPath(job2, job2FileInputPath);
		FileOutputFormat.setOutputPath(job2, job2FileOutputPath);

		job2.setMapperClass(SecondFilterMapper.class);
		job2.setReducerClass(SecondFilterReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.waitForCompletion(true);
		
	}
}

class FirstFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
	// get only the words that begins with an letter and ands with a vowel
	// most words Southern Soto ends with a vowel.
	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		for (String word : words){
			
			String wordResult = word.replaceAll("[-+.^:,()*&%!@#$]","");
			if (wordResult.length()>1){
				String reverseWord = new StringBuilder(wordResult.toLowerCase()).reverse().toString();
				char lastLetter = reverseWord.charAt(0);
				char firstLetter = wordResult.charAt(0);
				if (lastLetter == 'a' || lastLetter == 'e' || lastLetter == 'u' || lastLetter == 'i' || lastLetter == 'o'){
					if (firstLetter >= 'a' && firstLetter <= 'z'){
						context.write(new Text(wordResult), new Text());
					}
				}
			}
		}
	}
}

class FirstFilterReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
		context.write(key, new Text("okidoki23"));
		
	}
}




class SecondFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split("\\r?\\n");
		
		
		ArrayList<String> words = new ArrayList<String>();
		for (String s1 : lines){
			String word = s1.split("okidoki23")[0];
			String wordResult = word.replaceAll("[-+.^:,()*&%!@#$]","");
			wordResult = wordResult.replace("/t", "");
			wordResult = wordResult.replace("/n", "");
			wordResult = wordResult.replace(" ", "");
			words.add(wordResult);
		}
		
	
		for(String s2 : words){
			char[] chars = s2.toCharArray();
			for(int i=1; i<chars.length-1; i++){
				int prev = i-1;
				int next = i+1;

				if (prev >=0 && next <= chars.length-1){
					boolean prevChar = charIsVowel(s2.charAt(prev));
					boolean curChar = charIsVowel(s2.charAt(i));
					boolean nextChar = charIsVowel(s2.charAt(next));
					
					if (!prevChar && !curChar && !nextChar){
						context.write(new Text(s2), new Text(""));
					}
				}
			}
		}
	}
	
	private boolean charIsVowel(char c){
		return c == 'a' || c == 'e' || c == 'u' || c == 'i' || c == 'o';
	}
}

class SecondFilterReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		context.write(key, new Text(""));
		
	}
}
