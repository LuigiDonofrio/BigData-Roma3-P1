package it.uniroma3.MapReduce.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import it.uniroma3.MapReduce.ProductJoin.ProductUserMapper;
import it.uniroma3.MapReduce.ProductJoin.ProductUserReducer;

public class ProductJoin {
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf();
		conf.setJobName("Amazon Fine Food Reviews - Top 10 Words");
		
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		conf.setMapperClass(ProductUserMapper.class);
		conf.setReducerClass(ProductUserReducer.class);
		
		conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        
		conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        JobClient.runJob(conf);
	}
}
