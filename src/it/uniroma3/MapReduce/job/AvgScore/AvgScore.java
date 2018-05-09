package it.uniroma3.MapReduce.job.AvgScore;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import it.uniroma3.MapReduce.job.AvgScore.MR.AvgScoreMapper;
import it.uniroma3.MapReduce.job.AvgScore.MR.AvgScoreReducer;
import it.uniroma3.MapReduce.job.AvgScore.Types.YearScoreArrayWritable;
import it.uniroma3.MapReduce.job.AvgScore.Types.YearScoreWritable;

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
