package it.uniroma3.MapReduce.job.ProductJoin.MR;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import it.uniroma3.MapReduce.job.ProductJoin.Types.BiProductWritable;

public class ProductUserReducer extends Reducer<Text, Text, BiProductWritable, Text> {
	@SuppressWarnings("unused")
	private Logger log = Logger.getLogger(ProductUserReducer.class);
	
	@Override
	public void reduce(Text userId, Iterable<Text> productIds, Context ctx) throws IOException, InterruptedException {
		Set<String> products = new HashSet<>();
		
		for(Text t: productIds)
			products.add(t.toString());
		
		Set<List<String>> cartesian = Sets.cartesianProduct(ImmutableList.of(products, products));
		cartesian = cartesian.stream().map(x -> {
			List<String> mutable = new LinkedList<String>(x);
			mutable.sort((a, b) -> a.compareTo(b));
			return mutable;
		}).filter(x -> !x.get(0).equals(x.get(1))).collect(Collectors.toSet());
		
		if(cartesian.isEmpty())
			return; /* No more than 1 review */
		
		for(List<String> x : cartesian) 
			ctx.write(new BiProductWritable(new Text(x.get(0)), new Text(x.get(1))), new Text(userId));
	}
}
