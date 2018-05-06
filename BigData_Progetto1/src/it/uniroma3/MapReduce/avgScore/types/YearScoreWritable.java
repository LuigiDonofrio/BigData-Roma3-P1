package it.uniroma3.MapReduce.avgScore.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class YearScoreWritable implements WritableComparable<YearScoreWritable> {
	private Text year;
	private IntWritable score;
	
	public YearScoreWritable() {}
	
	public YearScoreWritable(Text year, IntWritable score) {
		this.year = year;
		this.score = score;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = new Text(in.readUTF());
		this.score = new IntWritable(in.readUnsignedShort());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.year.toString());
		out.writeShort(this.score.get());
	}

	@Override
	public int compareTo(YearScoreWritable that) {
		int this_year = Integer.parseInt(this.year.toString());
		int that_year = Integer.parseInt(that.year.toString());
		
		int this_score = this.score.get();
		int that_score = that.score.get();
		
		if (this_year > that_year)
			return 1;
		else
			if(this_year < that_year)
				return -1;
			else //Sono lo stesso anno
				if(this_score > that_score)
					return 1;
				else
					if(this_score < that_score)
						return -1;
					else
						return 0; //questo è il caso in cui sono uguali
	}
	
	@Override
	public boolean equals(Object obj) {
		YearScoreWritable that = (YearScoreWritable) obj;
		return (this.year.toString().equals(that.year.toString())) && (this.score.get()==that.score.get());
	}
	
	@Override
	public String toString() {
		return "(["+this.year.toString()+"]-"+this.score.get()+")";
	}
	
    @Override
    public int hashCode() {
        return this.score.hashCode() + this.year.hashCode();
    }

}
