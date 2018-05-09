package it.uniroma3.Spark.job.AvgScore;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

public class AvgScoreTask implements Serializable {
	private static final long serialVersionUID = -7480117781445167268L;
	
	private static final int MIN_YEAR = 2003;
	private static final int MAX_YEAR = 2012;
	
	public static List<Tuple2<Integer, Float>> getAvgByYear(Iterable<Tuple2<Integer, Integer>> it) {
		Map<Integer, List<Float>> year2avgScore = new HashMap<>();
		List<Tuple2<Integer, Float>> avgByYear = new LinkedList<>();
		
		it.forEach(x -> {
			int year = x._1();
			
			/* Pre-filtering on year value */
			if(year < MIN_YEAR || MAX_YEAR > 2012) 
				return;
				
			List<Float> scores = null;
			
			if(year2avgScore.containsKey(year))
				scores = year2avgScore.get(year);
			else
				scores = new LinkedList<>();
			
			scores.add((float) x._2());
			year2avgScore.put(year, scores);
		});
		
		year2avgScore.keySet().forEach(x -> {
			List<Float> scores = year2avgScore.get(x);
			float avg = scores.stream().reduce((a, b) -> a + b).get() / (int) scores.stream().count();
			
			avgByYear.add(new Tuple2<Integer, Float>(x, avg));
		});
		
		return avgByYear;
	}
}
