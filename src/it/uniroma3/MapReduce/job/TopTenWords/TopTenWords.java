package it.uniroma3.MapReduce.job.TopTenWords;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import it.uniroma3.MapReduce.job.TopTenWords.MR.TopTenWordsCombiner;
import it.uniroma3.MapReduce.job.TopTenWords.MR.TopTenWordsMapper;
import it.uniroma3.MapReduce.job.TopTenWords.MR.TopTenWordsReducer;
import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyArrayWritable;
import it.uniroma3.MapReduce.job.TopTenWords.Types.WordOccurencyWritable;

public class TopTenWords {
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf();
		conf.setJobName("Amazon Fine Food Reviews - Top 10 Words");
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        conf.setMapperClass(TopTenWordsMapper.class);
        conf.setCombinerClass(TopTenWordsCombiner.class);
        conf.setReducerClass(TopTenWordsReducer.class);
        
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(WordOccurencyWritable.class);
        
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(WordOccurencyArrayWritable.class);
        
        JobClient.runJob(conf);
	}
}
