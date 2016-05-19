package main;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import dkdtree.DKDTree;
import dkdtree.DKDtreeUtils;
import kdtree.Point;

public class BatchDriver {


	public static void main(String[] args) {
		// Logger.getLogger("org").setLevel(Level.OFF);
		// Logger.getLogger("akka").setLevel(Level.OFF);
		SparkConf sparkConf = new SparkConf()// .setMaster("local[4]")
				.setAppName("SparkStreamingKDTree")
				// .set("spark.eventLog.enabled", "false")
				// .set("spark.streaming.backpressure.enabled", "true")
		;
		String inPath = args[0];

		int k = Integer.parseInt(args[3]);
		int sampleSize = Integer.parseInt(args[4]);
		int numPartitions = Integer.parseInt(args[5]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> pointStrings = sc.textFile(inPath);
		JavaRDD<Point> points = DKDtreeUtils.pointFromString(pointStrings);

		JavaRDD<Point> ekNNRes = DKDTree.epsilonNeighborhoodKNNQuery(points, points, k, 0.05f, numPartitions,
				sampleSize);

		JavaRDD<Point> kNNRes = DKDTree.kNNQuery(points, points, k, numPartitions, sampleSize);

		System.out.println(ekNNRes.count());
		System.out.println(kNNRes.count());
		sc.stop();
		sc.close();

	}
}
