package it.uniroma3.MapReduce.job.AvgScore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.job.AvgScore.MR.AvgScoreMapper;
import it.uniroma3.MapReduce.job.AvgScore.MR.AvgScoreReducer;
import it.uniroma3.MapReduce.job.AvgScore.Types.YearScoreArrayWritable;
import it.uniroma3.MapReduce.job.AvgScore.Types.YearScoreWritable;

public class AvgScore {
	private static Logger log = Logger.getLogger(AvgScore.class);
	
	public static void main(String[] args) throws Exception {
		Job job = new Job(new Configuration(), "Amazon Fine Food Reviews - Average Score Per Year");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(AvgScoreMapper.class);
        job.setReducerClass(AvgScoreReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(YearScoreWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(YearScoreArrayWritable.class);
    
        long start = System.currentTimeMillis();
        job.setJarByClass(AvgScore.class); 
        job.waitForCompletion(true);
        log.info("Completed in: " + (System.currentTimeMillis() - start) + "ms");
	}
}
