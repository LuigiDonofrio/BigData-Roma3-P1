package it.uniroma3.MapReduce.TopTenWords;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.TopTenWords.types.WordOccurencyWritable;
import it.uniroma3.MapReduce.Util.AmazonReview;
import it.uniroma3.MapReduce.Util.CSVFieldsExtractor;
import it.uniroma3.MapReduce.Util.Stopworder;

public class TopTenWordsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, WordOccurencyWritable> {
	private Logger log = Logger.getLogger(TopTenWordsMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, WordOccurencyWritable> output, Reporter reporter) throws IOException {
		String csvLine = value.toString();
		
		AmazonReview review = null;
		
		try {
			CSVFieldsExtractor parser = new CSVFieldsExtractor();
			review = parser.extract(csvLine);
		} catch (Exception e) {
			log.info("Unable to parse row");
			return;
		}
		
		String[] words = review.getSummary().split("\\W+");
		Stopworder sw = Stopworder.getInstance();
		List<String> processed = sw.removeStopWords(Arrays.asList(words));
		
		for(String word : processed)
			output.collect(new Text(review.getYear()), new WordOccurencyWritable(new Text(word), new LongWritable(1)));
	}
}