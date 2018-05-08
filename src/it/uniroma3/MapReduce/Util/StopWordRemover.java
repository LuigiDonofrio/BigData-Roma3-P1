package it.uniroma3.MapReduce.Util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.io.CharStreams;

public class StopWordRemover {
	private static StopWordRemover instance;
	private List<String> stopwords;
	
	private StopWordRemover() throws IOException {
		loadStopWords();
	}
	
	public static StopWordRemover getInstance() throws IOException {
		instance = (instance == null) ? new StopWordRemover() : instance;
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
		words.stream().forEach(x -> {
			String purged = x.trim();
			
			if(purged.length() > 0)
				lowerCase.add(purged.toLowerCase());
		});
		List<String> processed = new ArrayList<>();
		lowerCase.stream().forEach(x -> { if(!this.stopwords.contains(x)) processed.add(x); });
		
		return processed;
	}
}
