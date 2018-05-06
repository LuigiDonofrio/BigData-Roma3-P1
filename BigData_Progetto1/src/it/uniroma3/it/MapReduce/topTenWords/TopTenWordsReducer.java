package it.uniroma3.it.MapReduce.topTenWords;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TopTenWordsReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
	@SuppressWarnings("unused")
	private Logger log = Logger.getLogger("TopeTenWordsReducer");
	
	@Override
	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		List<String> words = new LinkedList<>();
		values.forEachRemaining(x -> words.add(x.toString()));	
		Map<String, Integer> occourrenceMap = countWords(words);	
		cleanUp(key, output, occourrenceMap);
	}

	private Map<String, Integer> countWords (List<String> words) {	
		Map<String, Integer> occourrenceMap = new HashMap<>();
		words.stream().forEach(x -> { if (occourrenceMap.containsKey(x))
											occourrenceMap.put(x, occourrenceMap.get(x)+1);
									  else
											occourrenceMap.put(x, 1);	
		});
		return occourrenceMap;
	}
	
	private void cleanUp (Text year ,OutputCollector<Text, Text> output, Map<String, Integer> occourrenceMap) throws IOException {
        List<Map.Entry<String, Integer>> sortedMap = new ArrayList<>(occourrenceMap.entrySet());
        
        Collections.sort(sortedMap,new Comparator<Map.Entry<String, Integer>>() {
    		@Override
    		public int compare(Map.Entry<String, Integer> e1, Map.Entry<String, Integer> e2) {
    			return (e1.getValue()).compareTo(e2.getValue())*(-1);
    		}
    	});
        
    	StringBuilder builder = new StringBuilder();
    	for(int i = 0; i < 10; i++) {
    		if(sortedMap.size() > i)
    			builder.append(sortedMap.get(i).getKey().toString()+"("+sortedMap.get(i).getValue()+") ");
    	}
    	
    	output.collect(year, new Text(builder.toString()));
	}
}
