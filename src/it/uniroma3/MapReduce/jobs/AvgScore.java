package it.uniroma3.MapReduce.jobs;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import it.uniroma3.MapReduce.AvgScore.AvgScoreMapper;
import it.uniroma3.MapReduce.AvgScore.AvgScoreReducer;
import it.uniroma3.MapReduce.AvgScore.types.YearScoreArrayWritable;
import it.uniroma3.MapReduce.AvgScore.types.YearScoreWritable;

public class AvgScore {
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf();
		conf.setJobName("Amazon Fine Food Reviews - Average Score Per Year");
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
        conf.setMapperClass(AvgScoreMapper.class);
        conf.setReducerClass(AvgScoreReducer.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(YearScoreWritable.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(YearScoreArrayWritable.class);
    
        JobClient.runJob(conf);
	}
}
