package it.uniroma3.MapReduce.ProductJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.Util.AmazonReview;
import it.uniroma3.MapReduce.Util.CSVFieldsExtractor;

public class ProductUserMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	private Logger log = Logger.getLogger(ProductUserMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String csvLine = value.toString();
		
		AmazonReview review = null;
		
		try {
			CSVFieldsExtractor parser = new CSVFieldsExtractor();
			review = parser.extract(csvLine);
		} catch (Exception e) {
			log.info("Unable to parse row");
			return;
		}
		
		/* Trickage: Instead of having 2 mappers to do the join, duplicate the same output */
		/* The 1-2\t will be used in reducer to understand how to join values */
		/* from first "table" with the ones from the "second" table. */
		output.collect(new Text(review.getUserId()), new Text("1\t"+review.getProductId()));
		output.collect(new Text(review.getUserId()), new Text("2\t"+review.getProductId()));
	}
}