package it.uniroma3.MapReduce.job.ProductJoin.Types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import it.uniroma3.Utils.PatternFactory;

public class BiProductWritable implements WritableComparable<BiProductWritable> {
	private Text product1;
	private Text product2;
	
	public BiProductWritable() {}
	
	public BiProductWritable(Text p1, Text p2) {
		this.product1 = p1;
		this.product2 = p2;
	}
	
	public BiProductWritable(String serialized) {
		Pattern extractor = PatternFactory.getInstance().getPattern("biproduct");
		Matcher m = extractor.matcher(serialized);
		
		this.product1 = new Text("NULL");
		this.product2 = new Text("NULL");
		
		if(m.find()) {
			this.product1 = (m.group(1) != null) ? new Text(m.group(1)) : new Text("NULL");
			this.product2 = (m.group(2) != null) ? new Text(m.group(2)) : new Text("NULL");
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.product1 = new Text(in.readUTF());
		this.product2 = new Text(in.readUTF());
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.product1.toString());
		out.writeUTF(this.product2.toString());
	}

	@Override
	public int compareTo(BiProductWritable that) {
		String this_first = this.product1.toString();
		String that_first = that.getFirst().toString();
		
		String this_second = this.product2.toString();
		String that_second = that.getSecond().toString();
	
		return (this_first.equals(that_first)) ? this_second.compareTo(that_second) :
												 this_first.compareTo(that_first);
	}
	
	@Override
	public boolean equals(Object obj) {
		BiProductWritable that = (BiProductWritable) obj;
		return this.product1.toString().equals(that.getFirst().toString()) &&
			   this.product2.toString().equals(that.getSecond().toString());
	}
	
	@Override
	public String toString() {
		return "(" + this.product1.toString() + ", " + this.product2.toString() + ")";
	}
	
    @Override
    public int hashCode() {
        return this.product2.hashCode() + this.product1.hashCode();
    }

	public Text getFirst() {
		return product1;
	}

	public Text getSecond() {
		return product2;
	}
}
