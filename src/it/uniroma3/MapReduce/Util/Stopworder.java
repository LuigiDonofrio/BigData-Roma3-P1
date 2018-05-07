package it.uniroma3.MapReduce.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.io.CharStreams;

public class Stopworder {
	private static Stopworder instance;
	private List<String> stopwords;
	
	private Stopworder() throws IOException {
		loadStopWords();
	}
	
	public static Stopworder getInstance() throws IOException {
		instance = (instance == null) ? new Stopworder() : instance;
		return instance;
	}
	
	private void loadStopWords() throws IOException {
		InputStream stopWordsFile = getClass().getResourceAsStream("/stopwords-en.txt");
		
		String stopWordsLines = CharStreams.toString(new InputStreamReader(stopWordsFile, "UTF-8"));
		String[] stopwords = stopWordsLines.split("\n");
		
		this.stopwords = Arrays.asList(stopwords);
	}
	
	public List<String> removeStopWords(List<String> words) {
		List<String> lowerCase = new ArrayList<>();
		words.stream().forEach(x -> lowerCase.add(x.toLowerCase()));
		List<String> processed = new ArrayList<>();
		lowerCase.stream().forEach(x -> { if(!this.stopwords.contains(x)) processed.add(x); });
		
		return processed;
	}
}
