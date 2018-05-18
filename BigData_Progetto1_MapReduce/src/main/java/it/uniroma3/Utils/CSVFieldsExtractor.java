package it.uniroma3.Utils;

import java.io.Serializable;
import java.io.StringReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVFieldsExtractor implements Serializable {
	private static final long serialVersionUID = 3789585628851001350L;
	
	class ReviewFields {
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
	}
	
	public AmazonReview extract(String csvLine) throws Exception {
		AmazonReview review = new AmazonReview();
		
		StringReader reader = new StringReader(csvLine);
		CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT);
		
		CSVRecord line = parser.iterator().next();
		
		review.setId(Long.parseLong(line.get(ReviewFields.ID)));
		review.setProductId(line.get(ReviewFields.PRODUCT_ID));
		review.setUserId(line.get(ReviewFields.USER_ID));
		review.setProfileName(line.get(ReviewFields.PROFILE_NAME));
		review.setHelpfullnessNumerator(Integer.parseInt(line.get(ReviewFields.HELPFULLNESS_NUMERATOR)));
		review.setHelpfullnessDenominator(Integer.parseInt(line.get(ReviewFields.HELPFULLNESS_DENOMINATOR)));
			
		try {
			long time = Long.parseLong(line.get(ReviewFields.TIME)) * 1000L;
			
			Date parsedTime = new Date(time);
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");
			
			review.setYear(Integer.parseInt(dateFormat.format(parsedTime)));
		} catch(Exception e) {} 
		finally {
			review.setScore(Integer.parseInt(line.get(ReviewFields.SCORE)));
			review.setSummary(line.get(ReviewFields.SUMMARY));
			review.setText(line.get(ReviewFields.TEXT));
			
			parser.close();
		}
		
		return review;
	}
}
