package it.uniroma3.MapReduce.job.TopTenWords.MR;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyWritable;

/* Combiner has same output type of Mapper and same input type of Reducer. */
/* It isn't always executed so even if skipped, it won't change the Reducer's result. */
/* In this case it's different from Reducer's logic because we don't want to filter top 10 results */
/* but we only need to have a single key-value pair for a word and return a partial sum */
/* for a given data chunk. */
public class TopTenWordsCombiner extends Reducer<IntWritable, WordOccurencyWritable, IntWritable, WordOccurencyWritable> {
	@Override
	public void reduce(IntWritable year, Iterable<WordOccurencyWritable> word2occurency, Context ctx) throws IOException, InterruptedException {
		Map<String, Long> occourrenceMap = countWords(word2occurency);
		
		for(String key : occourrenceMap.keySet()) {
			Long occurencies = occourrenceMap.get(key);
			ctx.write(
					year, 
					new WordOccurencyWritable(new Text(key), new LongWritable(occurencies))
			);
		}
	}

	private Map<String, Long> countWords(Iterable<WordOccurencyWritable> word2occurency) {	
		Map<String, Long> occourrenceMap = new HashMap<>();
		
		word2occurency.forEach(x -> {
			if (occourrenceMap.containsKey(x.getWord().toString()))
				occourrenceMap.put(x.getWord().toString(), occourrenceMap.get(x.getWord().toString()) + x.getOccurency().get());
			else
				occourrenceMap.put(x.getWord().toString(), x.getOccurency().get());
		});
		
		return occourrenceMap;
	}
}
