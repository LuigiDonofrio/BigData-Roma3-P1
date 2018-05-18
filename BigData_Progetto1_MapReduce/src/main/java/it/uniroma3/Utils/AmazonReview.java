package it.uniroma3.Utils;

import java.io.Serializable;

public class AmazonReview implements Serializable {	
	private static final long serialVersionUID = -619157103139522242L;
	
	private long id;
	private String productId;
	private String userId;
	private String profileName;
	private int helpfullnessNumerator;
	private int helpfullnessDenominator;
	private int score;
	private int year;
	private String summary;
	private String text;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getProductId() {
		return productId;
	}
	public void setProductId(String productId) {
		this.productId = productId;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getProfileName() {
		return profileName;
	}
	public void setProfileName(String profileName) {
		this.profileName = profileName;
	}
	public int getHelpfullnessNumerator() {
		return helpfullnessNumerator;
	}
	public void setHelpfullnessNumerator(int helpfullnessNumerator) {
		this.helpfullnessNumerator = helpfullnessNumerator;
	}
	public int getHelpfullnessDenominator() {
		return helpfullnessDenominator;
	}
	public void setHelpfullnessDenominator(int helpfullnessDenominator) {
		this.helpfullnessDenominator = helpfullnessDenominator;
	}
	public int getScore() {
		return score;
	}
	public void setScore(int score) {
		this.score = score;
	}
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public String getSummary() {
		return summary;
	}
	public void setSummary(String summary) {
		this.summary = summary;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
}