package it.uniroma3.MapReduce.job.ProductJoin.MR;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.uniroma3.MapReduce.job.ProductJoin.Types.BiProductWritable;

public class ProductUserFinalReducer extends Reducer<BiProductWritable, Text, BiProductWritable, LongWritable> {
	@SuppressWarnings("unused")
	@Override
	public void reduce(BiProductWritable products, Iterable<Text> userIds, Context ctx) throws IOException, InterruptedException {
		long i = 0;
		
		for(Text u : userIds)
			++i;
		
		ctx.write(products, new LongWritable(i));
	}
}
