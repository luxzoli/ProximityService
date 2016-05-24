package main;

import kdtree.Point;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import dkdtree.DKDTree;
import dkdtree.DKDTreeUtils;

public class BatchDriver {


	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[4]")
				.setAppName("SparkStreamingKDTree")
				.set("spark.streaming.backpressure.enabled", "true")
		;
		String inPath = args[0];
		float epsilon = Float.parseFloat(args[1]);
		int k = Integer.parseInt(args[2]);
		int sampleSize = Integer.parseInt(args[3]);
		int numPartitions = Integer.parseInt(args[4]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> pointStrings = sc.textFile(inPath);
		JavaRDD<Point> points = DKDTreeUtils.pointFromString(pointStrings);

		JavaRDD<Point> ekNNRes = DKDTree.epsilonNeighborhoodKNNQuery(points, points, k, epsilon, numPartitions,
				sampleSize);

		//JavaRDD<Point> kNNRes = DKDTree.kNNQuery(points, points, k, numPartitions, sampleSize);

		System.out.println(ekNNRes.count());
		//System.out.println(kNNRes.count());
		sc.stop();
		sc.close();

	}
}
