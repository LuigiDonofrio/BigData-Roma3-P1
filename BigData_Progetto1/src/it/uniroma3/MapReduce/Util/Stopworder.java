package it.uniroma3.MapReduce.Util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.io.Files;

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
	
	@SuppressWarnings({"deprecation"})
	private void loadStopWords() throws IOException {
		File stopWordsFile = new File("/home/luigidonofrio/Scaricati/stop-word-list.csv");
		String csv = Files.readFirstLine(stopWordsFile, StandardCharsets.UTF_8);
		String[] stopwords = csv.split(",");
		this.stopwords = Arrays.asList(stopwords);
	}
	
	public List<String> removeStopWords (List<String> words) {
		List<String> processed = new ArrayList<>();
		words.stream().forEach(x -> processed.add(x.toLowerCase()));
		words.stream().forEach(x -> {if(this.stopwords.contains(x)) processed.remove(x);});
		return processed;
	}
	

}
