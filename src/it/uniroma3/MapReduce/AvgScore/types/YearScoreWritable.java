package it.uniroma3.MapReduce.AvgScore.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class YearScoreWritable implements WritableComparable<YearScoreWritable> {
	private IntWritable year;
	private FloatWritable score;
	
	public YearScoreWritable() {}
	
	public YearScoreWritable(IntWritable year, FloatWritable score) {
		this.year = year;
		this.score = score;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = new IntWritable(in.readUnsignedShort());
		this.score = new FloatWritable(in.readFloat());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(this.year.get());
		out.writeFloat(this.score.get());
	}

	@Override
	public int compareTo(YearScoreWritable that) {
		int this_year = this.year.get();
		int that_year = that.year.get();
		
		float this_score = this.score.get();
		float that_score = that.score.get();
		
		if(this_year > that_year)
			return 1;
		else if(this_year < that_year)
			return -1;
		else
			return (this_score > that_score) ? 1 : ((this_score < that_score) ? -1 : 0);
	}
	
	@Override
	public boolean equals(Object obj) {
		YearScoreWritable that = (YearScoreWritable) obj;
		return (this.year.toString().equals(that.year.toString())) && 
			   (this.score.get() == that.score.get());
	}
	
	@Override
	public String toString() {
		return "([" + this.year.toString() + "] - " + String.format("%.2f", this.score.get()) + ")";
	}
	
    @Override
    public int hashCode() {
        return this.score.hashCode() + this.year.hashCode();
    }

	public IntWritable getYear() {
		return year;
	}

	public FloatWritable getScore() {
		return score;
	}
}
