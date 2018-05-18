package it.uniroma3.MapReduce.job.TopTenWords;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.job.TopTenWords.MR.TopTenWordsCombiner;
import it.uniroma3.MapReduce.job.TopTenWords.MR.TopTenWordsMapper;
import it.uniroma3.MapReduce.job.TopTenWords.MR.TopTenWordsReducer;
import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyArrayWritable;
import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyWritable;

public class TopTenWords {
	private static Logger log = Logger.getLogger(TopTenWords.class);
	
	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration(), "Amazon Fine Food Reviews - Top 10 Words");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(TopTenWordsMapper.class);
        job.setCombinerClass(TopTenWordsCombiner.class);
        job.setReducerClass(TopTenWordsReducer.class);
        
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(WordOccurencyWritable.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(WordOccurencyArrayWritable.class);
        
        long start = System.currentTimeMillis();
        job.setJarByClass(TopTenWords.class); 
        job.waitForCompletion(true);
        log.info("Completed in: " + (System.currentTimeMillis() - start) + "ms");
	}
}
