package it.uniroma3.MapReduce.job.TopTenWords.MR;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyWritable;
import it.uniroma3.Utils.AmazonReview;
import it.uniroma3.Utils.CSVFieldsExtractor;
import it.uniroma3.Utils.StopWordRemover;

public class TopTenWordsMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, WordOccurencyWritable> {
	private Logger log = Logger.getLogger(TopTenWordsMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<IntWritable, WordOccurencyWritable> output, Reporter reporter) throws IOException {
		String csvLine = value.toString();
		
		AmazonReview review = null;
		
		try {
			CSVFieldsExtractor parser = new CSVFieldsExtractor();
			review = parser.extract(csvLine);
		} catch (Exception e) {
			log.info("Unable to parse row");
			return;
		}
		
		String[] words = review.getSummary().split("\\W+"); /* Splits on every non-word (dots, commas...) */
		StopWordRemover swRemover = StopWordRemover.getInstance();
		/* Removes stopwords so results are cleaner and we avoid stuff like "a", "the"... */
		List<String> processed = swRemover.removeStopWords(Arrays.asList(words));
		
		for(String word : processed)
			output.collect(new IntWritable(review.getYear()), new WordOccurencyWritable(new Text(word), new LongWritable(1)));
	}
}