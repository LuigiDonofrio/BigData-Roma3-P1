package it.uniroma3.MapReduce.ProductJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.Util.AmazonReview;
import it.uniroma3.MapReduce.Util.CSVFieldsExtractor;

public class ProductUserMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Logger log = Logger.getLogger(ProductUserMapper.class);
	
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
		
		ctx.write(new Text(review.getUserId()), new Text(review.getProductId()));
	}
}