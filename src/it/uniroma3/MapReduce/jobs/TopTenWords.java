package it.uniroma3.MapReduce.jobs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import it.uniroma3.MapReduce.TopTenWords.TopTenWordsCombiner;
import it.uniroma3.MapReduce.TopTenWords.TopTenWordsMapper;
import it.uniroma3.MapReduce.TopTenWords.TopTenWordsReducer;
import it.uniroma3.MapReduce.TopTenWords.types.WordOccurencyArrayWritable;
import it.uniroma3.MapReduce.TopTenWords.types.WordOccurencyWritable;

public class TopTenWords {
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf();
		conf.setJobName("Amazon Fine Food Reviews - Top 10 Words");
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        conf.setMapperClass(TopTenWordsMapper.class);
        conf.setCombinerClass(TopTenWordsCombiner.class);
        conf.setReducerClass(TopTenWordsReducer.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(WordOccurencyWritable.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(WordOccurencyArrayWritable.class);
        
        JobClient.runJob(conf);
	}
}
