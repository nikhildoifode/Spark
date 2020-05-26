import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Covid19_2 {
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Insufficient or Extra arguments.");
			System.err.println("Usage: spark-submit --class Covid19_2 SparkCovid19.jar <in> [population_file] <out>");
			System.exit(1);
		}

		Configuration hadoopConf = new Configuration();
		FileSystem hdfs = FileSystem.get(hadoopConf);
		String inputPath = args[0];
		String output = args[2];
		String cache = args[1];

		if (!(hdfs.exists(new Path(inputPath)) && hdfs.exists(new Path(cache)))) {
			System.out.println("Filepaths for input or population files don't exist.");
			System.exit(1);
		}

		if (hdfs.exists(new Path(output))) hdfs.delete(new Path(output), true);

		SparkConf conf = new SparkConf().setAppName("Covid19_2_Analysis");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> cacheFile = sc.textFile(cache);
		JavaPairRDD<String, Integer> populationValues = cacheFile.mapToPair(
			new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String value) {
				String[] row = value.split(",");
				if (row.length == 5 && !row[1].equals("location"))
					return new Tuple2<String, Integer>(row[1], Integer.valueOf(row[4]));
				else return new Tuple2<String, Integer>("", 0);
			}
		}).distinct();

		Broadcast<JavaPairRDD<String, Integer>> broadcastVar = sc.broadcast(populationValues);

		JavaRDD<String> dataRDD = sc.textFile(inputPath);
		JavaPairRDD <String, Double> counts = dataRDD.flatMapToPair(
			new PairFlatMapFunction <String, String, Integer>() {
			public Iterator<Tuple2<String, Integer>> call(String value) {
				String[] row = value.split(",");
				List<Tuple2<String, Integer>> retWords = new ArrayList<Tuple2<String, Integer>> ();

				if (!(row[0].equals("date")))
					retWords.add(new Tuple2<String, Integer>(row[1], Integer.valueOf(row[2])));

				return retWords.iterator();
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x+y;
			}
		}).join(broadcastVar.value()).mapToPair(
			new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Double>() {
			public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Integer>> t) {
				double d = (double) t._2()._1() / t._2()._2() * 1000000;
				return new Tuple2<String, Double>(t._1(), d);
			}
		});

		counts.saveAsTextFile(output);
	}
}