package it.uniroma3.MapReduce.TopTenWords;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.TopTenWords.types.WordOccurencyArrayWritable;
import it.uniroma3.MapReduce.TopTenWords.types.WordOccurencyWritable;

public class TopTenWordsReducer extends MapReduceBase implements Reducer<Text, WordOccurencyWritable, Text, WordOccurencyArrayWritable> {
	@SuppressWarnings("unused")
	private Logger log = Logger.getLogger(TopTenWordsReducer.class);
	
	@Override
	public void reduce(Text year, Iterator<WordOccurencyWritable> word2occurency, OutputCollector<Text, WordOccurencyArrayWritable> output, Reporter reporter) throws IOException {
		Map<String, Long> occourrenceMap = countWords(word2occurency);	
		
		this.writeTopTen(year, output, occourrenceMap);
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
	
	private void writeTopTen(Text year, OutputCollector<Text, WordOccurencyArrayWritable> output, Map<String, Long> occourrenceMap) throws IOException {
        List<Map.Entry<String, Long>> sortedMap = new ArrayList<>(occourrenceMap.entrySet());
        
        Collections.sort(sortedMap, new Comparator<Map.Entry<String, Long>>() {
    		@Override
    		public int compare(Map.Entry<String, Long> e1, Map.Entry<String, Long> e2) {
    			return (e1.getValue()).compareTo(e2.getValue()) * (-1);
    		}
    	});
        
        WordOccurencyWritable[] top10 = new WordOccurencyWritable[10];
        
    	int maxSize = (sortedMap.size() >= 10) ? 10 : sortedMap.size();
    	
    	for(int i = 0; i < maxSize; ++i)
    		top10[i] = new WordOccurencyWritable(new Text(sortedMap.get(i).getKey().toString()), new LongWritable(sortedMap.get(i).getValue()));
    	
    	output.collect(year, new WordOccurencyArrayWritable(top10));
	}
}
