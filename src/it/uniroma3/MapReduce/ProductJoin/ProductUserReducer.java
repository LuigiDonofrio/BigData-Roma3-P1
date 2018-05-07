package it.uniroma3.MapReduce.ProductJoin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class ProductUserReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	private Logger log = Logger.getLogger(ProductUserReducer.class);
	
	@Override
	public void reduce(Text userId, Iterator<Text> row, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		row.forEachRemaining(x -> log.info(x));
	}
}
