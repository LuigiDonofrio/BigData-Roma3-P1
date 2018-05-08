package it.uniroma3.MapReduce.ProductJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.ProductJoin.types.BiProductWritable;

public class ProductUserFinalReducer extends Reducer<BiProductWritable, Text, BiProductWritable, LongWritable> {
	@SuppressWarnings("unused")
	private Logger log = Logger.getLogger(ProductUserFinalReducer.class);
	
	@SuppressWarnings("unused")
	@Override
	public void reduce(BiProductWritable products, Iterable<Text> userIds, Context ctx) throws IOException, InterruptedException {
		long i = 0;
		
		for(Text u : userIds)
			++i;
		
		ctx.write(products, new LongWritable(i));
	}
}
