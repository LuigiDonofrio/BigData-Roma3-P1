package it.uniroma3.MapReduce.Util;

import static it.uniroma3.MapReduce.Util.Utils.HELPFULLNESS_DENOMINATOR;
import static it.uniroma3.MapReduce.Util.Utils.HELPFULLNESS_NUMERATOR;
import static it.uniroma3.MapReduce.Util.Utils.ID;
import static it.uniroma3.MapReduce.Util.Utils.PRODUCT_ID;
import static it.uniroma3.MapReduce.Util.Utils.PROFILE_NAME;
import static it.uniroma3.MapReduce.Util.Utils.SCORE;
import static it.uniroma3.MapReduce.Util.Utils.SUMMARY;
import static it.uniroma3.MapReduce.Util.Utils.TEXT;
import static it.uniroma3.MapReduce.Util.Utils.TIME;
import static it.uniroma3.MapReduce.Util.Utils.USER_ID;

import java.io.IOException;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

//TODO da provare
public class CSVFieldsExtractor {
	private Logger log = Logger.getLogger(CSVFieldsExtractor.class.getName());
	private static final String ID_DEFAULT = "NO_ID";
	private static final String PRODUCT_ID_DEFAULT = "NO_PRODUCT_ID";
	private static final String USER_ID_DEFAULT = "NO_USER_ID";
	private static final String PROFILE_NAME_DEFAULT = "NO_PRODILE_NAME";
	private static final String HELPFULLNESS_NUMERATOR_DEFAULT = "NO_HELPFULLNESS_NUMERATOR" ;
	private static final String HELPFULLNESS_DENOMINATOR_DEFAULT = "NO_HELPFULLNESS_DENOMINATOR";
	private static final String SCORE_DEFAULT = "NO_SCORE";
	private static final String TIME_DEFAULT = "NO_TIME";
	private static final String SUMMARY_DEFAULT = "NO_SUMMARY";
	private static final String TEXT_DEFAULT = "NO_TEXT";
	
	private String id;
	private String product_id;
	private String user_id;
	private String profile_name;
	private String helpfullness_numerator;
	private String helpfullness_denominator;
	private String score;
	private String time;
	private String summary;
	private String text;
	
	public CSVFieldsExtractor() {
		this.id = ID_DEFAULT;
		this.product_id = PRODUCT_ID_DEFAULT;
		this.user_id = USER_ID_DEFAULT;
		this.profile_name = PROFILE_NAME_DEFAULT;
		this.helpfullness_numerator = HELPFULLNESS_NUMERATOR_DEFAULT;
		this.helpfullness_denominator = HELPFULLNESS_DENOMINATOR_DEFAULT;
		this.score = SCORE_DEFAULT;
		this.time = TIME_DEFAULT;
		this.summary = SUMMARY_DEFAULT;
		this.text = TEXT_DEFAULT;
	}
	
	public List<String> extract(String csvLine) throws IOException {
		List<String> lineMap = new ArrayList<>();
		
		StringReader reader = new StringReader(csvLine);
		CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT);
		
		for(CSVRecord line : parser) {
			this.product_id = line.get(PRODUCT_ID);
			this.user_id = line.get(USER_ID);
			this.profile_name = line.get(PROFILE_NAME);
			this.id = line.get(ID);
			this.helpfullness_numerator = line.get(HELPFULLNESS_NUMERATOR);
			this.helpfullness_denominator = line.get(HELPFULLNESS_DENOMINATOR);
			
			/*
			 * Time parser
			 */
			try {
				long time = Long.parseLong(line.get(TIME)) * 1000L; //il timestamp è in Unix-Time quindi in secondi		
				Date parsedTime = new Date(time);
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy"); // the format of your date
				this.time = dateFormat.format(parsedTime);
			} catch (Exception e) {
				log.info("Unable to parse: "+line.get(TIME));
			}
		
			this.score = line.get(SCORE);
			this.summary = line.get(SUMMARY);
			this.text = line.get(TEXT);
		}
		parser.close();
		
		lineMap.add(this.id);						//0
		lineMap.add(this.product_id);				//1
		lineMap.add(this.user_id);					//2
		lineMap.add(this.profile_name);				//3
		lineMap.add(this.helpfullness_numerator);	//4
		lineMap.add(this.helpfullness_denominator);	//5
		lineMap.add(this.score);					//6
		lineMap.add(this.time);						//7
		lineMap.add(this.summary);					//8
		lineMap.add(this.text);						//9
		
		return lineMap;
	}

}
