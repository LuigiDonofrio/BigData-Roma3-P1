package it.uniroma3.MapReduce.avgScore;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.avgScore.types.YearScoreWritable;

public class AvgScoreReducer extends MapReduceBase implements Reducer<Text, YearScoreWritable, Text, Text> {
	
	private Logger log = Logger.getLogger(AvgScoreReducer.class);
	@Override
	public void reduce(Text product, Iterator<YearScoreWritable> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		log.info(product.toString()+" --- ");
		values.forEachRemaining(x -> System.out.println(x.toString()));
		//TODO Da completare
	}

}
