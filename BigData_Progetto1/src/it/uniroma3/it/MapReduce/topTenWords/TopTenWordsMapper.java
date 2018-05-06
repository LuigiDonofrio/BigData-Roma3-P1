package it.uniroma3.it.MapReduce.topTenWords;

import static it.uniroma3.it.MapReduce.Util.Utils.SUMMARY;
import static it.uniroma3.it.MapReduce.Util.Utils.TIME;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import it.uniroma3.it.MapReduce.Util.Stopworder;

public class TopTenWordsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	private Logger log = Logger.getLogger(TopTenWordsMapper.class);
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			/*
			 * Estrazione dei dati dal CSV
			 */
			String csvLine = value.toString();
			String[] fields = csvLine.split(",");
			
			//TODO capire bene come gestire questi anni problematici
			String year = "NO_YEAR";
			try {
				long time = Long.parseLong(fields[TIME]) * 1000L; //il timestamp è in Unix-Time quindi in secondi		
				Date parsedTime = new Date(time);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy"); // the format of your date
				year = (Integer.parseInt(dateFormat.format(parsedTime)) < 1999) ? dateFormat.format(parsedTime) : "NO_YEAR";
			} catch (Exception e) {
				log.info("Unable to parse: "+fields[TIME]);
			}
			
			/*
			 * Pre-processing del campo SUMMARY
			 */
			String summary = fields[SUMMARY];
			//TODO migliorare l'estrazione delle parole
			String[] words = summary.split("\\s+");
			Stopworder sw = Stopworder.getInstance();
			List<String> parole = sw.removeStopWords(Arrays.asList(words));
			
			for(String word: parole) {
				output.collect(new Text(year), new Text(word));
			}		
	}

}
