package main;

import geo.dkdtree.GeoDKDTree;
import geo.dkdtree.GeoDKDTreeUtils;
import geo.kdtree.GeoPoint;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class GeoDriver {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[4]")
				.setAppName("SparkStreamingKDTree")
				.set("spark.streaming.backpressure.enabled", "true");
		String inPath = args[0];
		float epsilon = Float.parseFloat(args[1]);
		int k = Integer.parseInt(args[2]);
		int sampleSize = Integer.parseInt(args[3]);
		int numPartitions = Integer.parseInt(args[4]);
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc,
				Durations.milliseconds(10000));
		JavaDStream<String> inputStream = ssc.socketTextStream("localhost",
				13000);
		JavaRDD<String> pointStrings = sc.textFile(inPath);

		JavaRDD<GeoPoint> pointsNF = GeoDKDTreeUtils.pointFromString(pointStrings);
		JavaRDD<GeoPoint> points = GeoDKDTreeUtils.filterGeoPoints(pointsNF);
		GeoDKDTree dtree = new GeoDKDTree(points, sampleSize, numPartitions, epsilon);
		JavaDStream<GeoPoint> pointsStream = GeoDKDTreeUtils.pointFromStringStream(inputStream);

		JavaPairDStream<Integer, GeoPoint> tKNNDStream = dtree
				.streamEpsilonNeighborhoodKNNQuery(pointsStream, k, epsilon);
		JavaDStream<GeoPoint> knnDStream = GeoDKDTreeUtils.undoPair(tKNNDStream);
		knnDStream.count().print();
		ssc.start();
		ssc.awaitTerminationOrTimeout(180000);
		ssc.stop();
		ssc.close();
		sc.stop();

	}
}
