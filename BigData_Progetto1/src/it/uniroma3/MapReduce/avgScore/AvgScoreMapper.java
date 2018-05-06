package it.uniroma3.MapReduce.avgScore;

import static it.uniroma3.MapReduce.Util.Utils.PRODUCT_ID;
import static it.uniroma3.MapReduce.Util.Utils.SCORE;
import static it.uniroma3.MapReduce.Util.Utils.TIME;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import it.uniroma3.MapReduce.avgScore.types.YearScoreWritable;

public class AvgScoreMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, YearScoreWritable>{
	private Logger log = Logger.getLogger(AvgScoreMapper.class.getName());
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, YearScoreWritable> out, Reporter reporter)
			throws IOException {
		String csvLine = value.toString();
		//String[] fields = csvLine.split(",");

		StringReader reader = new StringReader(csvLine);
		CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT);
		
		String year = "NO_YEAR";
		String prodId = "NO_PROD";
		int score = -1;
		
		//TODO capire bene come gestire questi anni problematici
		//TODO codice duplicato bisogna metterlo nelle utils
		for(CSVRecord line : parser) {
			try {
				long time = Long.parseLong(line.get(TIME)) * 1000L; //il timestamp è in Unix-Time quindi in secondi		
				Date parsedTime = new Date(time);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy"); // the format of your date
				year = dateFormat.format(parsedTime);
			} catch (Exception e) {
				log.info("Unable to parse: "+line.get(TIME));
			}
		
			prodId = line.get(PRODUCT_ID);
			
			try {
				score = Integer.parseInt(line.get(SCORE));
			} catch (Exception e) {
				log.info("Unable to parse to int: "+line.get(SCORE));
			}
		}
		
		parser.close();
		out.collect(new Text(prodId), new YearScoreWritable(new Text(year), new IntWritable(score)));		
	
	}
	

}
