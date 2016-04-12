package main.java.nl.hu.hadoop.wordcount;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Scanner;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.google.inject.Key;
import com.sun.tools.javac.util.List;
import com.sun.xml.internal.xsom.impl.scd.Iterators.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

public class WordFrequencie {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(WordFrequencie.class);

		FileInputFormat.addInputPath(job, new Path("alice.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapperClass(WordFrequencieMapper.class);
		job.setReducerClass(WordFrequencieReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		freqSelect("/Users/dannyhorvath/Downloads/BigData-CharFrequency-master/frequency/output/part-r-00000");
		readFile("/Users/dannyhorvath/Downloads/BigData-CharFrequency-master/frequency/bigshuf.txt");
	}

	public static void freqSelect(String fileName) throws IOException {
		String line = null;
		HashMap<Character, HashMap<Character, Integer>> charFrequentieUsed = new HashMap<Character, HashMap<Character, Integer>>();
		try {
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				String lineWithoutSpaces = line.replaceAll("\\s+", "");
				char charEen = lineWithoutSpaces.charAt(0);
				char charTwee = lineWithoutSpaces.charAt(1);
				String str = lineWithoutSpaces.replaceAll("[^0-9.,]+", "");
				if (!charFrequentieUsed.containsKey(charEen)) {
					charFrequentieUsed.put(charEen, new HashMap<Character, Integer>());
				}
				charFrequentieUsed.get(charEen).put(charTwee, Integer.parseInt(str));

			}
			bufferedReader.close();
			System.out.println("Ending FreqSelect");
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		}
	}

	public static boolean isAlpha(String name) {
		return name.matches("[a-zA-Z]+");
	}

	public static void readFile(String fileName) throws IOException{
		try {
			String line = null;
			String nonEnglish = "";
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			while ((line = bufferedReader.readLine()) != null) {
				line = line.replaceAll("[-+.^:,'_\"=]","");
				line = line.toLowerCase();
				String words[] = line.split("\\s+");
				for (int i = 0; i < words.length; i++) {
					if(isAlpha(words[i])) {
						double chance = predict(words[i]);
						if (chance < 5) {
							nonEnglish = nonEnglish + " " +words[i] + "\n";
						}
					}
				}
			}
			System.out.println(nonEnglish);
			fileReader.close();
			bufferedReader.close();
		}
		catch(FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		}
	}

	public static double predict(String word){
		try {
			String letterCombination = "";
			double totalWordProbability = 0;
			//System.out.println("\n\nWoord: " + word + " Length: " + word.length() + " Eerste letter: " + word.charAt(0));
			for (int i = 0; i < word.length()-1; i++) {
				//System.out.println("I val: "+ i);
				letterCombination = new StringBuilder().append(word.charAt(i)).append(word.charAt(i+1)).toString();
				//System.out.println("Lettercombination: "+ letterCombination);
				totalWordProbability += (getProbability(letterCombination)/(word.length()-1));
				//System.out.println("TotalProb: "+totalWordProbability);
			}
			return totalWordProbability;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static double getProbability(String letterCombination) throws IOException {
		try {
			String line = null;
			double sum = 0;
			double letterCombinationTimes = 0;
			FileReader fileReader = new FileReader("/Users/dannyhorvath/Downloads/BigData-CharFrequency-master/frequency/output/part-r-00000");
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			while ((line = bufferedReader.readLine()) != null) {
				String words[] = line.split("\t");
				if(letterCombination.length() == 2) {
					if (words[0].equals(letterCombination)) {
						letterCombinationTimes = Double.parseDouble(words[1]);
					} else if (words[0].charAt(0) == (letterCombination.charAt(0)) && words[0].charAt(1) != (letterCombination.charAt(1))) {
						sum = sum + Double.parseDouble(words[1]);
					}
				}
			}
			fileReader.close();
			bufferedReader.close();
			sum += letterCombinationTimes;

			double probability = (letterCombinationTimes / sum) * 100;
			//System.out.println("Letter times: " + letterCombinationTimes + " Sum: " + sum + " Prob: " + probability);
			return probability;
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file 'output.txt'");
		}
		return 0;
	}
}

class WordFrequencieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split("\\s");
		String normalWord;
		String tweetal = "";
		char vorigeChar; 
		for (String theWord: words) {           
			normalWord = theWord.replaceAll("[^a-zA-Z]", "");
			normalWord = normalWord.toLowerCase();
			for(int i=0; i<normalWord.length(); i++){
				if(i==0){
					tweetal = "";
				}
				tweetal += normalWord.charAt(i);
				vorigeChar = normalWord.charAt(i);
				if(tweetal.length()==2){
					context.write(new Text(tweetal), new IntWritable(1));
					tweetal = "";
					tweetal += vorigeChar;
				}
				
			}
	    }
		
	}
}
  

class WordFrequencieReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	int arrayCount = 0;
	private static HashMap<Character, HashMap<Character,Integer>> charFrequentieUsed = new HashMap<Character, HashMap<Character,Integer>>();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		for(IntWritable val: values){
			arrayCount +=1;
		}
		String line = key.toString();
		context.write(new Text(line), new IntWritable(arrayCount));
		arrayCount =0;

    }
	

}
