package it.uniroma3.MapReduce.job.AvgScore.MR;

import java.io.IOException;
import org.apache.log4j.Logger;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.uniroma3.MapReduce.job.AvgScore.Types.YearScoreWritable;
import it.uniroma3.Utils.AmazonReview;
import it.uniroma3.Utils.CSVFieldsExtractor;

public class AvgScoreMapper extends Mapper<LongWritable, Text, Text, YearScoreWritable> {
	private Logger log = Logger.getLogger(AvgScoreMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
		String csvLine = value.toString();
		
		AmazonReview review = null;
		
		try {
			CSVFieldsExtractor parser = new CSVFieldsExtractor();
			review = parser.extract(csvLine);
		} catch (Exception e) {
			log.info("Unable to parse row");
			return;
		}

		ctx.write(
			new Text(review.getProductId()), 
			new YearScoreWritable(new IntWritable(review.getYear()), new FloatWritable(review.getScore()))
		);
	}
}