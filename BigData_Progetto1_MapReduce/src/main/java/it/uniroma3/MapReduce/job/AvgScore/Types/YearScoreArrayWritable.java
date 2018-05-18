package it.uniroma3.MapReduce.job.AvgScore.Types;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;

public class YearScoreArrayWritable extends ArrayWritable {
    public YearScoreArrayWritable(YearScoreWritable[] values) {
        super(YearScoreWritable.class, values);
    }

    @Override
    public YearScoreWritable[] get() {
        return (YearScoreWritable[]) super.get();
    }

    @Override
    public String toString() {
    	YearScoreWritable[] values = this.get();
        
    	StringBuilder builder = new StringBuilder("");
    	Arrays.asList(values).stream().forEach(x -> {
        	builder.append(x.toString() + " ");
        });
    	
    	return builder.toString();
    }
}