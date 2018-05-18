package it.uniroma3.MapReduce.job.TopTenWords.MR;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyWritable;
import it.uniroma3.Utils.AmazonReview;
import it.uniroma3.Utils.CSVFieldsExtractor;
import it.uniroma3.Utils.StopWordRemover;

public class TopTenWordsMapper extends Mapper<LongWritable, Text, IntWritable, WordOccurencyWritable> {
	private Logger log = Logger.getLogger(TopTenWordsMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
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
			ctx.write(new IntWritable(review.getYear()), new WordOccurencyWritable(new Text(word), new LongWritable(1)));
	}
}