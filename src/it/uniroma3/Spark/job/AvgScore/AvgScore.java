package it.uniroma3.Spark.job.AvgScore;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import it.uniroma3.Utils.AmazonReview;
import it.uniroma3.Utils.CSVFieldsExtractor;
import scala.Tuple2;

public class AvgScore {
	private String inputPath;
	private String outputPath;

	public AvgScore(String inputPath,  String outputPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
	}

	@SuppressWarnings("resource")
	private JavaRDD<AmazonReview> loadData() {
		SparkConf conf = new SparkConf()
							.setAppName("Amazon Fine Food Reviews - Average Score Per Year");
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

	private void getAvgsByProductId() {
		JavaRDD<AmazonReview> reviews = loadData();

		reviews
		.filter(x -> x != null)
		.mapToPair(x -> new Tuple2<String, Tuple2<Integer, Integer>>(
				x.getProductId(), 
				new Tuple2<Integer, Integer>(
						x.getYear(), 
						x.getScore()
				)
		))
		.filter(x -> x._1() != null)
		.groupByKey()
	    .mapToPair(x -> {
	    	return new Tuple2<String, List<Tuple2<Integer, Float>>>(x._1(), AvgScoreTask.getAvgByYear(x._2()));
	    })
	    .sortByKey()
	    .coalesce(1)
	    .saveAsTextFile(this.outputPath);
	}
	
	public static void main(String[] args) {
		if(args.length < 2) { 
			System.err.println("Usage: AvgScore <csvdataset> <outputpath>"); 
			System.exit(1);
		}

		new AvgScore(args[0], args[1]).getAvgsByProductId();
	}
}
