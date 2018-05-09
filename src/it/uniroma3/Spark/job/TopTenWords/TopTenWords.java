package it.uniroma3.Spark.job.TopTenWords;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import it.uniroma3.Utils.AmazonReview;
import it.uniroma3.Utils.CSVFieldsExtractor;
import it.uniroma3.Utils.StopWordRemover;
import scala.Tuple2;

public class TopTenWords {
	private String inputPath;
	private String outputPath;

	public TopTenWords(String inputPath, String outputPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}

	@SuppressWarnings("resource")
	private JavaRDD<AmazonReview> loadData() {
		SparkConf conf = new SparkConf()
							 .setAppName("Amazon Fine Food Reviews - Top 10 Words");
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

	private void getTopTenWordsPerYear() {
		JavaRDD<AmazonReview> reviews = loadData();
		
		reviews
		.filter(x -> x != null)
		.mapToPair(x -> new Tuple2<Integer, List<String>> (
							x.getYear(), 
							StopWordRemover.getInstance().removeStopWords(Arrays.asList(x.getSummary().split("\\W+")))
						)
		)
		.flatMapValues(x -> x)
		.groupByKey()
		.mapToPair(x -> new Tuple2<Integer, List<Tuple2<String, Long>>>(x._1(), TopTenWordsTask.getWordOccurencies(x._2())))
		.mapValues(x -> x.stream().limit(10).collect(Collectors.toList()))
		.sortByKey(true)
		.coalesce(1)
		.saveAsTextFile(this.outputPath);
	}
	
	public static void main(String[] args) {
		if(args.length < 2) { 
			System.err.println("Usage: TopTenWords <csvdataset> <outputpath>"); 
			System.exit(1);
		}

		new TopTenWords(args[0], args[1]).getTopTenWordsPerYear();
	}
}
