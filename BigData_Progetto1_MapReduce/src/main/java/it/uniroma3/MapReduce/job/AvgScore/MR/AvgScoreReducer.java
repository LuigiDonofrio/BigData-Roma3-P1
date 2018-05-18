package it.uniroma3.MapReduce.job.AvgScore.MR;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.uniroma3.MapReduce.job.AvgScore.Types.YearScoreArrayWritable;
import it.uniroma3.MapReduce.job.AvgScore.Types.YearScoreWritable;

public class AvgScoreReducer extends Reducer<Text, YearScoreWritable, Text, YearScoreArrayWritable> {
	private static final int MIN_YEAR = 2003;
	private static final int MAX_YEAR = 2012;
	
	@Override
	public void reduce(Text productId, Iterable<YearScoreWritable> year2score, Context ctx) throws IOException, InterruptedException {
		Map<Integer, List<Float>> year2avg = this.getAvgScoreByYear(year2score);
		
		this.writeOutput(productId, year2avg, ctx);
	}

	private Map<Integer, List<Float>> getAvgScoreByYear(Iterable<YearScoreWritable> year2score) {
		Map<Integer, List<Float>> year2avgScore = new HashMap<>();
		
		year2score.forEach(x -> {
			int year = x.getYear().get();
			
			/* Pre-filtering on year value */
			if(year < MIN_YEAR || year > MAX_YEAR) 
				return;
				
			List<Float> scores = null;
			
			if(year2avgScore.containsKey(year))
				scores = year2avgScore.get(year);
			else
				scores = new LinkedList<>();
			
			scores.add((float) x.getScore().get());
			year2avgScore.put(year, scores);
		});

		year2avgScore.keySet().forEach(x -> {
			year2avgScore.compute(x, (k, v) -> {
				List<Float> avg = new LinkedList<>();
				avg.add(v.stream().reduce((a, b) -> a + b).get() / (int) v.stream().count());
				return avg;
			});
		});
		
		return year2avgScore;
	}
	
	private void writeOutput(Text productId, Map<Integer, List<Float>> year2avg, Context ctx) throws IOException, InterruptedException {
        YearScoreWritable[] year2avgWritable = new YearScoreWritable[year2avg.size()];
        
        int i = 0;
        for(int k : year2avg.keySet()) {
        	/* Only one value is stored in this List, because it's computed in getAvgScoreByYear */
        	float v = year2avg.get(k).get(0);
        
        	year2avgWritable[i] = new YearScoreWritable(
    			new IntWritable(k),
    			new FloatWritable(v)
    		);
            	
            ++i;
        }
            	
    	ctx.write(productId, new YearScoreArrayWritable(year2avgWritable));
	}
}
