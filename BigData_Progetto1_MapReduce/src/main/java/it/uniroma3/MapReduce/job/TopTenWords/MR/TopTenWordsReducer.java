package it.uniroma3.MapReduce.job.TopTenWords.MR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyArrayWritable;
import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyWritable;

public class TopTenWordsReducer extends Reducer<IntWritable, WordOccurencyWritable, IntWritable, WordOccurencyArrayWritable> {
	private static final int TOP_K = 10;
	
	@Override
	public void reduce(IntWritable year, Iterable<WordOccurencyWritable> word2occurency, Context ctx) throws IOException, InterruptedException {
		Map<String, Long> occourrenceMap = countWords(word2occurency);	
		
		this.writeTopTen(year, ctx, occourrenceMap);
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
	
	private void writeTopTen(IntWritable year, Context ctx, Map<String, Long> occourrenceMap) throws IOException, InterruptedException {
        List<Map.Entry<String, Long>> sortedMap = new ArrayList<>(occourrenceMap.entrySet());
        
        Collections.sort(sortedMap, new Comparator<Map.Entry<String, Long>>() {
    		@Override
    		public int compare(Map.Entry<String, Long> e1, Map.Entry<String, Long> e2) {
    			return (e1.getValue()).compareTo(e2.getValue()) * (-1);
    		}
    	});
        
    	int maxSize = (sortedMap.size() >= TOP_K) ? TOP_K : sortedMap.size();
    	WordOccurencyWritable[] top10 = new WordOccurencyWritable[maxSize];
    	
    	for(int i = 0; i < maxSize; ++i)
    		top10[i] = new WordOccurencyWritable(new Text(sortedMap.get(i).getKey().toString()), new LongWritable(sortedMap.get(i).getValue()));
    	
    	ctx.write(year, new WordOccurencyArrayWritable(top10));
	}
}
