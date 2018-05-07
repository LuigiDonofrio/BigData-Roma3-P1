package it.uniroma3.MapReduce.TopTenWords;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.TopTenWords.types.WordOccurencyWritable;

public class TopTenWordsCombiner extends MapReduceBase implements Reducer<Text, WordOccurencyWritable, Text, WordOccurencyWritable> {
	@SuppressWarnings("unused")
	private Logger log = Logger.getLogger(TopTenWordsCombiner.class);
	
	@Override
	public void reduce(Text year, Iterator<WordOccurencyWritable> word2occurency, OutputCollector<Text, WordOccurencyWritable> output, Reporter reporter) throws IOException {
		Map<String, Long> occourrenceMap = countWords(word2occurency);
		
		for(String key : occourrenceMap.keySet()) {
			Long occurencies = occourrenceMap.get(key);
			
			output.collect(new Text(year), new WordOccurencyWritable(new Text(key), new LongWritable(occurencies)));
		}
	}

	private Map<String, Long> countWords(Iterator<WordOccurencyWritable> word2occurency) {	
		Map<String, Long> occourrenceMap = new HashMap<>();
		
		word2occurency.forEachRemaining(x -> {
			if (occourrenceMap.containsKey(x.getWord().toString()))
				occourrenceMap.put(x.getWord().toString(), occourrenceMap.get(x.getWord().toString()) + x.getOccurency().get());
			else
				occourrenceMap.put(x.getWord().toString(), 1l);
		});
		
		return occourrenceMap;
	}
}