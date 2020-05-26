import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Date;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Covid19_3 {
	private static boolean world;
	private static Date dec31 = null;

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Insufficient or Extra arguments.");
			System.err.println("Usage: spark-submit --class Covid19_3 SparkCovid19.jar <in> [true|false] <out>");
			System.exit(1);
		}

		if (!(args[1].equals("true") || args[1].equals("false"))) {
			System.out.println("Invalid arguments. Either true or false");
			System.exit(1);
		}

		Configuration hadoopConf = new Configuration();
		FileSystem hdfs = FileSystem.get(hadoopConf);
		String inputPath = args[0];
		String output = args[2];
		SimpleDateFormat userInputDateFormat = new SimpleDateFormat("yyyy-MM-dd");

		if (!hdfs.exists(new Path(inputPath))) {
			System.out.println(inputPath + " doesn't exist.");
			System.exit(1);
		}

		if (hdfs.exists(new Path(output))) hdfs.delete(new Path(output), true);

		try {
			world = Boolean.parseBoolean(args[1]);
			dec31 = userInputDateFormat.parse("2019-12-31");
		} catch (ParseException e) {
			e.printStackTrace();
		}

		SparkConf conf = new SparkConf().setAppName("Covid19_3_Analysis");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> dataRDD = sc.textFile(inputPath);

		JavaPairRDD <String, Integer> counts = dataRDD.flatMapToPair(
			new PairFlatMapFunction <String, String, Integer>() {
			public Iterator<Tuple2<String, Integer>> call(String value) {
				String[] row = value.split(",");
				Date current = null;
				List<Tuple2<String, Integer>> retWords = new ArrayList<Tuple2<String, Integer>> ();
				SimpleDateFormat userInputDateFormat1 = new SimpleDateFormat("yyyy-MM-dd");

				if (!(row[0].equals("date") || (!world && (row[1].equals("World") || row[1].equals("International"))))) {
					try {
						current = userInputDateFormat1.parse(row[0]);
						if (!current.equals(dec31))
							retWords.add(new Tuple2<String, Integer>(row[1], Integer.valueOf(row[2])));
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}

				return retWords.iterator();
		    }
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x+y;
			}
		});

		counts.saveAsTextFile(output);
	}
}