package it.uniroma3.MapReduce.job.ProductJoin.MR;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.job.ProductJoin.Types.BiProductWritable;

public class ProductUserFinalMapper extends Mapper<LongWritable, Text, BiProductWritable, Text> {
	@SuppressWarnings("unused")
	private Logger log = Logger.getLogger(ProductUserFinalMapper.class);
	
	
	@Override
	public void map(LongWritable id, Text products2userId, Context ctx) throws IOException, InterruptedException {
		String line = products2userId.toString();
		String[] parts = line.split("\t");
		
		ctx.write(new BiProductWritable(parts[0]), new Text(parts[1]));
	}
}