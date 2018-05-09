package it.uniroma3.MapReduce.job.ProductJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import it.uniroma3.MapReduce.job.ProductJoin.MR.ProductUserFinalMapper;
import it.uniroma3.MapReduce.job.ProductJoin.MR.ProductUserFinalReducer;
import it.uniroma3.MapReduce.job.ProductJoin.MR.ProductUserMapper;
import it.uniroma3.MapReduce.job.ProductJoin.MR.ProductUserReducer;
import it.uniroma3.MapReduce.job.ProductJoin.Types.BiProductWritable;

public class ProductJoin {
	public static void main(String[] args) throws Exception {
		runJoinJob(args);
        runFinalJob(args);
	}
	
	private static void runJoinJob(String[] args) throws Exception {
		Job job = new Job(new Configuration(), "Amazon Fine Food Reviews - Product Join");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "_temp"));
		
		job.setMapperClass(ProductUserMapper.class);
		job.setReducerClass(ProductUserReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
		job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.waitForCompletion(true);
	}
	
	private static void runFinalJob(String[] args) throws Exception {
		Job job = new Job(new Configuration(), "Amazon Fine Food Reviews - Product Final");
		
		FileInputFormat.addInputPath(job, new Path(args[1] + "_temp"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ProductUserFinalMapper.class);
		job.setReducerClass(ProductUserFinalReducer.class);
		
		job.setMapOutputKeyClass(BiProductWritable.class);
        job.setMapOutputValueClass(Text.class);
        
		job.setOutputKeyClass(BiProductWritable.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.waitForCompletion(true);
	}
}
