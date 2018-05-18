package it.uniroma3.MapReduce.job.TopTenWords.Types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordOccurencyWritable implements WritableComparable<WordOccurencyWritable> {
	private Text word;
	private LongWritable occurency;
	
	public WordOccurencyWritable() {}
	
	public WordOccurencyWritable(Text word, LongWritable occurency) {
		this.word = word;
		this.occurency = occurency;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.word = new Text(in.readUTF());
		this.occurency = new LongWritable(in.readLong());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.word.toString());
		out.writeLong(this.occurency.get());
	}

	@Override
	public int compareTo(WordOccurencyWritable that) {
		long this_occurency = this.occurency.get();
		long that_occurency = that.occurency.get();
		
		String this_word = this.word.toString();
		String that_word = that.word.toString();
		
		if(this_word.equals(that_word))
			return (this_occurency > that_occurency) ? 1 : ((this_occurency < that_occurency) ? -1 : 0);
		else 
			return this_word.compareTo(that_word);
	}
	
	@Override
	public boolean equals(Object obj) {
		WordOccurencyWritable that = (WordOccurencyWritable) obj;
		return (this.word.toString().equals(that.word.toString())) && 
			   (this.occurency.get() == that.occurency.get());
	}
	
	@Override
	public String toString() {
		return "([" + this.word.toString() + "] - " + this.occurency.get() + ")";
	}
	
    @Override
    public int hashCode() {
        return this.word.hashCode() + this.occurency.hashCode();
    }

	public LongWritable getOccurency() {
		return occurency;
	}

	public Text getWord() {
		return word;
	}
}
