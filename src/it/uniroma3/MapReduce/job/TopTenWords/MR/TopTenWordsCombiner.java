package it.uniroma3.MapReduce.job.TopTenWords.MR;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyWritable;

/* Combiner has same output type of Mapper and same input type of Reducer. */
/* It isn't always executed so even if skipped, it won't change the Reducer's result. */
/* In this case it's different from Reducer's logic because we don't want to filter top 10 results */
/* but we only need to have a single key-value pair for a word and return a partial sum */
/* for a given data chunk. */
public class TopTenWordsCombiner extends MapReduceBase implements Reducer<IntWritable, WordOccurencyWritable, IntWritable, WordOccurencyWritable> {
	@SuppressWarnings("unused")
	private Logger log = Logger.getLogger(TopTenWordsCombiner.class);
	
	@Override
	public void reduce(IntWritable year, Iterator<WordOccurencyWritable> word2occurency, OutputCollector<IntWritable, WordOccurencyWritable> output, Reporter reporter) throws IOException {
		Map<String, Long> occourrenceMap = countWords(word2occurency);
		
		for(String key : occourrenceMap.keySet()) {
			Long occurencies = occourrenceMap.get(key);
			output.collect(year, new WordOccurencyWritable(new Text(key), new LongWritable(occurencies)));
		}
	}

	private Map<String, Long> countWords(Iterator<WordOccurencyWritable> word2occurency) {	
		Map<String, Long> occourrenceMap = new HashMap<>();
		
		word2occurency.forEachRemaining(x -> {
			if (occourrenceMap.containsKey(x.getWord().toString()))
				occourrenceMap.put(x.getWord().toString(), occourrenceMap.get(x.getWord().toString()) + x.getOccurency().get());
			else
				occourrenceMap.put(x.getWord().toString(), x.getOccurency().get());
		});
		
		return occourrenceMap;
	}
}
