package it.uniroma3.MapReduce.Util;

import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVFieldsExtractor {
	private Logger log = Logger.getLogger(CSVFieldsExtractor.class.getName());

	public static final int ID = 0;
	public static final int PRODUCT_ID = 1;
	public static final int USER_ID = 2;
	public static final int PROFILE_NAME = 3;
	public static final int HELPFULLNESS_NUMERATOR = 4;
	public static final int HELPFULLNESS_DENOMINATOR = 5;
	public static final int SCORE = 6;
	public static final int TIME = 7;
	public static final int SUMMARY = 8;
	public static final int TEXT = 9;
	
	public AmazonReview extract(String csvLine) throws Exception {
		AmazonReview review = new AmazonReview();
		
		StringReader reader = new StringReader(csvLine);
		CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT);
		
		CSVRecord line = parser.iterator().next();
		
		review.setId(Long.parseLong(line.get(ID)));
		review.setProductId(line.get(PRODUCT_ID));
		review.setUserId(line.get(USER_ID));
		review.setProfileName(line.get(PROFILE_NAME));
		review.setHelpfullnessNumerator(Integer.parseInt(line.get(HELPFULLNESS_NUMERATOR)));
		review.setHelpfullnessDenominator(Integer.parseInt(line.get(HELPFULLNESS_DENOMINATOR)));
			
		try {
			// Il timestamp è in Unix-Time quindi in secondi
			long time = Long.parseLong(line.get(TIME)) * 1000L;
			
			Date parsedTime = new Date(time);
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");
			
			review.setYear(dateFormat.format(parsedTime));
		} catch(Exception e) {
			log.info("Unable to parse: " + line.get(TIME));
		} finally {
			review.setScore(Integer.parseInt(line.get(SCORE)));
			review.setSummary(line.get(SUMMARY));
			review.setText(line.get(TEXT));
			
			parser.close();
		}
		
		return review;
	}
}
