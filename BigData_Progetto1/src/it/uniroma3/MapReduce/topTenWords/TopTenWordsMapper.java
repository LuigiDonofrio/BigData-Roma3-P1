package it.uniroma3.MapReduce.topTenWords;

import static it.uniroma3.MapReduce.Util.Utils.SUMMARY;
import static it.uniroma3.MapReduce.Util.Utils.TIME;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.MapReduce.Util.Stopworder;

public class TopTenWordsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	private Logger log = Logger.getLogger(TopTenWordsMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			/*
			 * Estrazione dei dati dal CSV
			 */
			String csvLine = value.toString();
			
			StringReader reader = new StringReader(csvLine);
			CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT);
			
			String year = "NO_YEAR";
			
			for(CSVRecord line: parser) {
				try {
					long time = Long.parseLong(line.get(TIME)) * 1000L; //il timestamp è in Unix-Time quindi in secondi		
					Date parsedTime = new Date(time);
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy"); // the format of your date
					year = dateFormat.format(parsedTime);
				} catch (Exception e) {
					log.info("Unable to parse: "+line.get(TIME));
				}
				
				String summary = line.get(SUMMARY);
				//TODO migliorare l'estrazione delle parole
				String[] words = summary.split("\\s+");
				Stopworder sw = Stopworder.getInstance();
				List<String> parole = sw.removeStopWords(Arrays.asList(words));
				
				for(String word: parole) {
					output.collect(new Text(year), new Text(word));
				}
				
			}
					
	}

}
