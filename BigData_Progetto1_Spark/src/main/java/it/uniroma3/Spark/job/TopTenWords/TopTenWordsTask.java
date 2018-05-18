package it.uniroma3.Spark.job.TopTenWords;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

public class TopTenWordsTask implements Serializable {
	private static final long serialVersionUID = -6786472401159421724L;
	
	public static List<Tuple2<String, Long>> getWordOccurencies(Iterable<String> it) {
		Map<String, Long> occourrenceMap = new HashMap<>();
		List<Tuple2<String, Long>> sortedMap = new LinkedList<Tuple2<String, Long>>();
        
		it.forEach(x -> { 
			if (occourrenceMap.containsKey(x))
				occourrenceMap.put(x, occourrenceMap.get(x) + 1l);
			else
				occourrenceMap.put(x, 1l);
		});
		
		occourrenceMap.keySet().forEach(x -> {
			Long occurrencies = occourrenceMap.get(x);
			sortedMap.add(new Tuple2<String, Long>(x, occurrencies));
		});
		
		Collections.sort(sortedMap, new Comparator<Tuple2<String, Long>>() {
    		@Override
    		public int compare(Tuple2<String, Long> e1, Tuple2<String, Long> e2) {
    			return (e1._2()).compareTo(e2._2()) * (-1);
    		}
    	});
		
		return sortedMap;
	}
}
