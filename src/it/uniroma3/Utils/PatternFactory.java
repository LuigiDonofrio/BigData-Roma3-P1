package it.uniroma3.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author luigidonofrio
 *
 */
public class PatternFactory {
	private static PatternFactory instance = null;
	
	private Map<String, Pattern> key2pattern = new HashMap<String, Pattern>();

	private PatternFactory() {
		key2pattern.put("biproduct", Pattern.compile("\\((.*),\\s+(.*)\\)"));
	}

	public static PatternFactory getInstance() {
		return (instance = (instance == null) ? new PatternFactory() : instance);
	}
	
	public Pattern getPattern(String key) {
		return (key2pattern.containsKey(key)) ? key2pattern.get(key) : null;
	}
}
