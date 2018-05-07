package it.uniroma3.MapReduce.AvgScore;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import it.uniroma3.MapReduce.AvgScore.types.YearScoreWritable;
import it.uniroma3.MapReduce.Util.AmazonReview;
import it.uniroma3.MapReduce.Util.CSVFieldsExtractor;

public class AvgScoreMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, YearScoreWritable>{
	private Logger log = Logger.getLogger(AvgScoreMapper.class.getName());
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, YearScoreWritable> out, Reporter reporter) throws IOException {
		String csvLine = value.toString();
		
		AmazonReview review = null;
		
		try {
			CSVFieldsExtractor parser = new CSVFieldsExtractor();
			review = parser.extract(csvLine);
		} catch (Exception e) {
			log.info("Unable to parse row");
			return;
		}

		out.collect(
			new Text(review.getProductId()), 
			new YearScoreWritable(new IntWritable(Integer.parseInt(review.getYear())), new FloatWritable(review.getScore()))
		);
	}
}