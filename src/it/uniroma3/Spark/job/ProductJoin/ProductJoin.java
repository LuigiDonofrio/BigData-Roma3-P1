package it.uniroma3.Spark.job.ProductJoin;

import static it.uniroma3.Spark.job.ProductJoin.SerializableComparator.serialize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import it.uniroma3.Utils.AmazonReview;
import it.uniroma3.Utils.CSVFieldsExtractor;
import scala.Tuple2;

public class ProductJoin {
	private String inputPath;
	private String outputPath;

	public ProductJoin(String inputPath, String outputPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}

	@SuppressWarnings("resource")
	private JavaRDD<AmazonReview> loadData() {
		SparkConf conf = new SparkConf()
							 .setAppName("Amazon Fine Food Reviews - Product Join");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		final CSVFieldsExtractor parser = new CSVFieldsExtractor();

		return sc.textFile(this.inputPath).map(line -> {
			try {
				return parser.extract(line);
			} catch(Exception e) {
				return null;
			}
		}).cache();
	}
	
	private void getProductsWithSameUsers() {
		JavaRDD<AmazonReview> reviews = loadData();
		
		JavaPairRDD<String, String> user2product = reviews.filter(x -> x != null)		
														  .mapToPair(x -> new Tuple2<String, String> (x.getUserId(), x.getProductId()));
		
		user2product
		.join(user2product)
		.filter(x -> !x._2()._1().equals(x._2()._2()))
		.mapValues(x -> (x._1().compareTo(x._2()) == 1) ? new Tuple2<>(x._2(), x._1()) : x)
		.distinct()
		.mapToPair(x -> new Tuple2<Tuple2<String, String>, String>(x._2(), x._1()))
		.groupByKey()
		.mapToPair(x -> new Tuple2<Tuple2<String, String>, Long>(x._1(), (long) Lists.newArrayList(x._2()).size()))
		.sortByKey(serialize((a, b) -> a._1().compareTo(b._1())))
		.coalesce(1)
		.saveAsTextFile(this.outputPath);
	}
	
	public static void main(String[] args) {
		if(args.length < 2) { 
			System.err.println("Usage: ProductJoin <csvdataset> <outputpath>"); 
			System.exit(1);
		}

		new ProductJoin(args[0], args[1]).getProductsWithSameUsers();
	}
}
