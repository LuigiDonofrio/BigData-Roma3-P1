package it.uniroma3.MapReduce.AvgScore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.AvgScore.types.YearScoreArrayWritable;
import it.uniroma3.MapReduce.AvgScore.types.YearScoreWritable;

public class AvgScoreReducer extends MapReduceBase implements Reducer<Text, YearScoreWritable, Text, YearScoreArrayWritable> {
	@SuppressWarnings("unused")
	private Logger log = Logger.getLogger(AvgScoreReducer.class);
	
	@Override
	public void reduce(Text productId, Iterator<YearScoreWritable> year2score, OutputCollector<Text, YearScoreArrayWritable> output, Reporter reporter) throws IOException {
		Map<Integer, List<Float>> year2avg = this.getAvgScoreByYear(year2score);
		
		this.writeOutput(productId, year2avg, output);
	}

	private Map<Integer, List<Float>> getAvgScoreByYear(Iterator<YearScoreWritable> year2score) {
		Map<Integer, List<Float>> year2avgScore = new HashMap<>();
		
		year2score.forEachRemaining(x -> {
			int year = x.getYear().get();
			
			if(year < 2003 || year > 2012) 
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
	
	private void writeOutput(Text productId, Map<Integer, List<Float>> year2avg, OutputCollector<Text, YearScoreArrayWritable> output) throws IOException {
        YearScoreWritable[] year2avgWritable = new YearScoreWritable[year2avg.size()];
        
        int i = 0;
        for(int k : year2avg.keySet()) {
        	float v = year2avg.get(k).get(0);
        
        	year2avgWritable[i] = new YearScoreWritable(
    			new IntWritable(k),
    			new FloatWritable(v)
    		);
            	
            ++i;
        }
            	
    	output.collect(productId, new YearScoreArrayWritable(year2avgWritable));
	}
}
