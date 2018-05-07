package it.uniroma3.MapReduce.TopTenWords.types;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;

public class WordOccurencyArrayWritable extends ArrayWritable {

    public WordOccurencyArrayWritable(WordOccurencyWritable[] values) {
        super(WordOccurencyWritable.class, values);
    }

    @Override
    public WordOccurencyWritable[] get() {
        return (WordOccurencyWritable[]) super.get();
    }

    @Override
    public String toString() {
    	WordOccurencyWritable[] values = this.get();
        
    	StringBuilder builder = new StringBuilder("");
    	Arrays.asList(values).stream().forEach(x -> {
        	builder.append(x.toString() + " ");
        });
    	
    	return builder.toString();
    }
}