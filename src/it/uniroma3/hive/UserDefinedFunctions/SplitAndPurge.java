package it.uniroma3.hive.UserDefinedFunctions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import it.uniroma3.MapReduce.Util.StopWordRemover;

public class SplitAndPurge extends UDF {
	public ArrayList<String> evaluate(final Text text) throws IOException {
		if(text == null) return null;
		
		String line = text.toString();
		
		String[] words = line.split("\\W+");
		StopWordRemover swRemover = StopWordRemover.getInstance();
		List<String> purged = swRemover.removeStopWords(Arrays.asList(words));
		
		return new ArrayList<>(purged);
	}
}