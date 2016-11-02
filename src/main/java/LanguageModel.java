import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;

import java.io.IOException;
import java.util.*;
import java.util.PriorityQueue;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threshold;
		// get the threashold parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threshold = conf.getInt("threshold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);
			
			if(count < threshold) {
				return;
			}
			
			//this is --> cool = 20
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length-1; i++) {
				sb.append(words[i]).append(" ");
			}
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1];
			
			if(!((outputKey == null) || (outputKey.length() <1))) {
				context.write(new Text(outputKey), new Text(outputValue + "=" + count));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Comparator<WordFrequencyPair> cmp = new Comparator<WordFrequencyPair>() {

				public int compare(WordFrequencyPair o1, WordFrequencyPair o2) {
					return o2.frequency - o1.frequency;
				}
			};

			PriorityQueue<WordFrequencyPair> pq = new PriorityQueue<WordFrequencyPair>(100, cmp);
			for (Text value: values) {
				String curValue = value.toString().trim();
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("=")[1].trim());
				WordFrequencyPair pair = new WordFrequencyPair(word, count);
				pq.offer(pair);

			}

			int count = 0;
			while(!pq.isEmpty()) {
				WordFrequencyPair pair = pq.poll();
				context.write(new DBOutputWritable(key.toString(), pair.getWord(), pair.getFrequency()),NullWritable.get());
				count++;
				if(count > n) {
					break;
				}
			}

		}
	}
}
