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

public class Covid19_1 {
	private static Date start = null, end = null;

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Insufficient or Extra arguments.");
			System.err.println("Usage: spark-submit --class Covid19_1 SparkCovid19.jar <in> [start_date yyyy-MM-dd] [end_date yyyy-MM-dd] <out>");
			System.exit(1);
		}

		Configuration hadoopConf = new Configuration();
		FileSystem hdfs = FileSystem.get(hadoopConf);
		String inputPath = args[0];
		String output = args[3];
		SimpleDateFormat userInputDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date low = null, high = null;

		if (!hdfs.exists(new Path(inputPath))) {
			System.out.println(inputPath + " doesn't exist.");
			System.exit(1);
		}

		if (hdfs.exists(new Path(output))) hdfs.delete(new Path(output), true);

		try {
    		low = userInputDateFormat.parse("2019-12-31");
			high = userInputDateFormat.parse("2020-04-08");
			start = userInputDateFormat.parse(args[1]);
			end = userInputDateFormat.parse(args[2]);

			if (end.after(high) || end.before(low) || start.before(low) || start.after(high) || start.after(end)) {
				System.err.println("Data only available from 2019-12-31 to 2020-04-08. Input proper range");
				System.exit(1);
			}
		} catch (ParseException e) {
			System.err.println("Date formats are invalid! It should be yyyy-MM-dd");
			System.exit(1);
		}

		SparkConf conf = new SparkConf().setAppName("Covid19_1_Analysis");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> dataRDD = sc.textFile(inputPath);

		JavaPairRDD <String, Integer> counts = dataRDD.flatMapToPair(
			new PairFlatMapFunction <String, String, Integer>() {
			public Iterator<Tuple2<String, Integer>> call(String value) {
				String[] row = value.split(",");
				Date current = null;
				List<Tuple2<String, Integer>> retWords = new ArrayList<Tuple2<String, Integer>> ();
				SimpleDateFormat userInputDateFormat1 = new SimpleDateFormat("yyyy-MM-dd");

				if (!(row[0].equals("date"))) {
					try {
						current = userInputDateFormat1.parse(row[0]);
						if ((current.after(start) && current.before(end)) || current.equals(start) || current.equals(end))
							retWords.add(new Tuple2<String, Integer>(row[1], Integer.valueOf(row[3])));
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